from collections import namedtuple, defaultdict
import copy
import datetime
import functools
import logging
import os
import re
from typing import Callable, Tuple, List

from flask import Flask, request, url_for, jsonify, send_from_directory, abort, make_response, Blueprint, g, \
    current_app, redirect
import pkg_resources
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.middleware.proxy_fix import ProxyFix

from openeo import ImageCollection
from openeo.capabilities import ComparableVersion
from openeo.error_summary import ErrorSummary
from openeo.util import date_to_rfc3339, dict_no_none, deep_get
from openeo_driver.backend import ServiceMetadata, BatchJobMetadata, get_backend_implementation
from openeo_driver.errors import OpenEOApiException, ProcessGraphMissingException, ServiceNotFoundException, \
    FilePathInvalidException
from openeo_driver.ProcessGraphDeserializer import evaluate, get_process_registry
from openeo_driver.save_result import SaveResult
from openeo_driver.users import HttpAuthHandler, User
from openeo_driver.utils import replace_nan_values

_log = logging.getLogger(__name__)

ApiVersionInfo = namedtuple("ApiVersionInfo", ["version", "supported", "advertised", "production"])

# Available OpenEO API versions: map of URL version component to API version info
API_VERSIONS = {
    "0.3.0": ApiVersionInfo(version="0.3.0", supported=False, advertised=False, production=False),
    "0.3.1": ApiVersionInfo(version="0.3.1", supported=False, advertised=False, production=False),
    "0.3": ApiVersionInfo(version="0.3.1", supported=False, advertised=False, production=False),
    "0.4.0": ApiVersionInfo(version="0.4.0", supported=True, advertised=True, production=True),
    "0.4.1": ApiVersionInfo(version="0.4.1", supported=True, advertised=True, production=True),
    "0.4.2": ApiVersionInfo(version="0.4.2", supported=True, advertised=True, production=True),
    "0.4": ApiVersionInfo(version="0.4.2", supported=True, advertised=True, production=True),
    "1.0.0": ApiVersionInfo(version="1.0.0", supported=True, advertised=True, production=False),
    "1.0": ApiVersionInfo(version="1.0.0", supported=True, advertised=True, production=False),
}
DEFAULT_VERSION = '0.4.2'

_log.info("API Versions: {v}".format(v=API_VERSIONS))
_log.info("Default API Version: {v}".format(v=DEFAULT_VERSION))

app = Flask(__name__)

# Make sure app handles reverse proxy aspects (e.g. HTTPS) correctly.
app.wsgi_app = ProxyFix(app.wsgi_app)

# TODO: get this OpenID config url from a real config
app.config['OPENID_CONNECT_CONFIG_URL'] = "https://sso-dev.vgt.vito.be/auth/realms/terrascope/.well-known/openid-configuration"

auth_handler = HttpAuthHandler()

openeo_bp = Blueprint('openeo', __name__)

backend_implementation = get_backend_implementation()


@openeo_bp.url_defaults
def _add_version(endpoint, values):
    """Blueprint.url_defaults handler to automatically add "version" argument in `url_for` calls."""
    if 'version' not in values and current_app.url_map.is_endpoint_expecting(endpoint, 'version'):
        values['version'] = g.get('request_version', DEFAULT_VERSION)


@openeo_bp.url_value_preprocessor
def _pull_version(endpoint, values):
    """Get API version from request and store in global context"""
    version = values.pop('version', DEFAULT_VERSION)
    if not (version in API_VERSIONS and API_VERSIONS[version].supported):
        raise OpenEOApiException(
            status_code=501,
            code="UnsupportedApiVersion",
            message="Unsupported version: {v!r}.  Available versions: {s!r}".format(
                v=version, s=[k for k, v in API_VERSIONS.items() if v.advertised]
            )
        )
    g.request_version = version
    g.api_version = API_VERSIONS[version].version


def requested_api_version() -> ComparableVersion:
    """Get the currently requested API version as a ComparableVersion object"""
    return ComparableVersion(g.api_version)


@openeo_bp.before_request
def _before_request():
    # Log some info about request
    data = request.data
    if len(data) > 1000:
        data = repr(data[:1000] + b'...') + ' ({b} bytes)'.format(b=len(data))
    else:
        data = repr(data)
    _log.info("Handling {method} {url} with data {data}".format(
        method=request.method, url=request.url, data=data
    ))


@app.errorhandler(HTTPException)
def handle_http_exceptions(error: HTTPException):
    # Convert to OpenEOApiException based handling
    return handle_openeoapi_exception(OpenEOApiException(
        message=str(error),
        code="NotFound" if isinstance(error, NotFound) else "Internal",
        status_code=error.code
    ))


@app.errorhandler(OpenEOApiException)
def handle_openeoapi_exception(error: OpenEOApiException):
    error_dict = error.to_dict()
    _log.error(str(error_dict), exc_info=True)
    return jsonify(error_dict), error.status_code


@app.errorhandler(Exception)
def handle_error(error: Exception):
    # TODO: convert to OpenEOApiException based handling
    error = backend_implementation.summarize_exception(error)

    if isinstance(error, ErrorSummary):
        return _error_response(error, 400, error.summary) if error.is_client_error \
            else _error_response(error, 500, error.summary)

    return _error_response(error, 500)


def _error_response(error: Exception, status_code: int, summary: str = None):
    # TODO: convert to OpenEOApiException based handling
    error_json = {
        "message": summary if summary else str(error)
    }
    if type(error) is HTTPException and type(error.response) is dict:
        error_json = error.response
    if type(error) is ErrorSummary:
        exception = error.exception
    else:
        exception = error

    current_app.logger.error(exception, exc_info=True)

    response = jsonify(error_json)
    response.status_code = status_code
    return response


def response_204_no_content():
    return make_response('', 204, {"Content-Type": "application/json"})


class EndpointRegistry:
    """
    Registry of OpenEO API endpoints, to be used as decorator with flask view functions.

    Allows setting some additional metadata and automatic generation of
    the OpenEO API endpoints listing in the "capabilities" endpoint.
    """

    EndpointMetadata = namedtuple("EndpointMetadata", ["hidden", "for_version"])

    def __init__(self):
        self._endpoints = {}

    def add_endpoint(self, view_func: Callable, hidden=False, version: Callable = None):
        """Register endpoint metadata"""
        self._endpoints[view_func.__name__] = self.EndpointMetadata(hidden=hidden, for_version=version)
        return view_func

    def __call__(self, view_func: Callable = None, *, hidden=False, version: Callable = None):
        if view_func is None:
            # Decorator with arguments: return wrapper to call with decorated function.
            return functools.partial(self.add_endpoint, hidden=hidden, version=version)
        else:
            # Argument-less decorator call: we already have the function to wrap, use default options.
            return self.add_endpoint(view_func)

    def get_path_metadata(self, blueprint: Blueprint) -> List[Tuple[str, set, EndpointMetadata]]:
        """
        Join registered blueprint routes with endpoint metadata
        and get a listing of (path, methods, metadata) tuples
        :return:
        """
        app = Flask("dummy")
        app.register_blueprint(blueprint)
        metadata = []
        for rule in app.url_map.iter_rules():
            if rule.endpoint.startswith(blueprint.name + '.'):
                name = rule.endpoint.split('.', 1)[1]
                if name in self._endpoints:
                    metadata.append((rule.rule, rule.methods.difference({'HEAD', 'OPTIONS'}), self._endpoints[name]))
        return metadata

    @staticmethod
    def get_capabilities_endpoints(metadata: List[Tuple[str, set, EndpointMetadata]], api_version) -> List[dict]:
        """
        Extract "capabilities" endpoint listing from metadata list
        """
        endpoint_methods = defaultdict(set)
        for path, methods, info in metadata:
            if not info.hidden and (info.for_version is None or info.for_version(api_version)):
                endpoint_methods[path].update(methods)
        endpoints = [
            {"path": p.replace('<', '{').replace('>', '}'), "methods": list(ms)}
            for p, ms in endpoint_methods.items()
        ]
        return endpoints


# Endpoint registry, to be used as decorator too
api_endpoint = EndpointRegistry()


@openeo_bp.route('/')
def index():
    app_config = current_app.config

    api_version = requested_api_version().to_string()
    title = app_config.get('OPENEO_TITLE', 'OpenEO API')
    service_id = app_config.get('OPENEO_SERVICE_ID', re.sub(r"\s+", "", title.lower() + '-' + api_version))
    # TODO only list endpoints that are actually supported by the backend.
    endpoints = EndpointRegistry.get_capabilities_endpoints(_openeo_endpoint_metadata, api_version=api_version)
    deploy_metadata = app_config.get('OPENEO_BACKEND_DEPLOY_METADATA') \
                      or build_backend_deploy_metadata(packages=["openeo", "openeo_driver"])

    capabilities = {
        "version": api_version,  # Deprecated pre-0.4.0 API version field
        "api_version": api_version,  # API version field since 0.4.0
        "backend_version": app_config.get('OPENEO_BACKEND_VERSION', '0.0.1'),
        "stac_version": "0.9.0",
        "id": service_id,
        "title": title,
        "description": app_config.get('OPENEO_DESCRIPTION', 'OpenEO API'),
        "production": API_VERSIONS[api_version].production,
        "endpoints": endpoints,
        "billing": {
            "currency": "EUR",
            "plans": [
                {
                    "name": "free",
                    "description": "Free plan. No limits!",
                    "url": "http://openeo.org/plans/free-plan",
                    "paid": False
                }
            ]
        },
        "_backend_deploy_metadata": deploy_metadata
    }

    return jsonify(capabilities)


def build_backend_deploy_metadata(packages: List[str]) -> dict:
    version_info = {}
    for package in packages:
        try:
            version_info[package] = str(pkg_resources.get_distribution(package))
        except pkg_resources.DistributionNotFound:
            version_info[package] = "n/a"
    return {
        'date': date_to_rfc3339(datetime.datetime.utcnow()),
        'versions': version_info
    }


@openeo_bp.route('/health')
def health():
    return jsonify({
        "health": backend_implementation.health_check()
    })


@api_endpoint(version=ComparableVersion("0.3.1").or_lower)
@openeo_bp.route('/capabilities')
def capabilities():
    return jsonify([
      "/data",
      "/execute",
      "/processes"
    ])


@api_endpoint(version=ComparableVersion("1.0.0").accept_lower)
@openeo_bp.route('/output_formats')
def output_formats():
    # TODO deprecated endpoint, remove it when v0.4 API support is not necessary anymore
    return jsonify(backend_implementation.file_formats()["output"])


@api_endpoint(version=ComparableVersion("1.0.0").or_higher)
@openeo_bp.route('/file_formats')
def file_formats():
    return jsonify(backend_implementation.file_formats())


@api_endpoint
@openeo_bp.route('/udf_runtimes' )
def udf_runtimes():
    return jsonify({
      "Python": {
        "description": "Predefined Python runtime environment.",
        "default": "latest",
        "versions": {
            "3.5.1":{
                "libraries":{
                    "numpy":{
                        "version":"1.14.3"
                    },
                    "pandas": {
                        "version": "0.22.0"
                    },
                    "tensorflow":{
                        "version":"1.11.0"
                    }
                }

            }
        }
      }

    })


@api_endpoint
@openeo_bp.route("/credentials/basic", methods=["GET"])
@auth_handler.requires_http_basic_auth
def credentials_basic():
    access_token, user_id = auth_handler.authenticate_basic(request)
    resp = {"access_token": access_token}
    if requested_api_version().below("1.0.0"):
        resp["user_id"] = user_id
    return jsonify(resp)


@api_endpoint
@openeo_bp.route("/credentials/oidc", methods=["GET"])
@auth_handler.public
def credentials_oidc():
    return redirect(current_app.config["OPENID_CONNECT_CONFIG_URL"])


@api_endpoint
@openeo_bp.route("/me", methods=["GET"])
@auth_handler.requires_bearer_auth
def me(user: User):
    return jsonify({
        "user_id": user.user_id,
        "info": user.info,
        # TODO more fields
    })


@openeo_bp.route('/timeseries' )
def timeseries():
    # TODO: deprecated? do we still need this endpoint? #35
    return 'OpenEO GeoPyspark backend. ' + url_for('.point')


@openeo_bp.route('/timeseries/point', methods=['POST'])
def point():
    # TODO: deprecated? do we still need this endpoint? #35
    x = float(request.args.get('x', ''))
    y = float(request.args.get('y', ''))
    srs = request.args.get('srs', None)
    process_graph = _extract_process_graph(request.json)
    image_collection = evaluate(process_graph, viewingParameters={'version': g.api_version})
    return jsonify(image_collection.timeseries(x, y, srs))


@openeo_bp.route('/download' , methods=['GET', 'POST'])
def download():
    # TODO: deprecated?
    if request.method == 'POST':
        outputformat = request.args.get('outputformat', 'geotiff')

        process_graph = request.get_json()
        image_collection = evaluate(process_graph)
        # TODO Unify with execute?
        filename = image_collection.download(None,outputformat=outputformat)

        return send_from_directory(os.path.dirname(filename),os.path.basename(filename))
    else:
        return 'Usage: Download image using POST.'


def _extract_process_graph(post_data: dict) -> dict:
    """
    Extract process graph dictionary from POST data

    see https://github.com/Open-EO/openeo-api/pull/262
    """
    try:
        if requested_api_version().at_least("1.0.0"):
            return post_data["process"]["process_graph"]
        else:
            # API v0.4 style
            return post_data['process_graph']
    except KeyError:
        raise ProcessGraphMissingException


@api_endpoint
@openeo_bp.route('/result', methods=['POST'])
def result():
    return execute()


@api_endpoint(version=ComparableVersion("0.3.1").or_lower)
@openeo_bp.route('/preview', methods=['GET', 'POST'])
def preview():
    # TODO: is this an old endpoint/shortcut or a custom extension of the API?
    return execute()


@openeo_bp.route('/execute', methods=['POST'])
def execute():
    # TODO:  This is not an official endpoint, does this "/execute" still have to be exposed as route?
    post_data = request.get_json()
    process_graph = _extract_process_graph(post_data)
    result = evaluate(process_graph, viewingParameters={'version': g.api_version})

    # TODO unify all this output handling within SaveResult logic?
    if isinstance(result, ImageCollection):
        format_options = post_data.get('output', {})
        filename = result.download(None, bbox="", time="", **format_options)
        return send_from_directory(os.path.dirname(filename), os.path.basename(filename))
    elif result is None:
        abort(500, "Process graph evaluation gave no result")
    elif isinstance(result, SaveResult):
        return result.create_flask_response()
    else:
        return jsonify(replace_nan_values(result))


@api_endpoint
@openeo_bp.route('/jobs', methods=['POST'])
@auth_handler.requires_bearer_auth
def create_job(user: User):
    # TODO: wrap this job specification in a 1.0-style ProcessGrahpWithMetadata?
    post_data = request.get_json()
    process = {"process_graph": _extract_process_graph(post_data)}
    job_options = post_data.get("job_options")
    job_info = backend_implementation.batch_jobs.create_job(
        user_id=user.user_id,
        process=process,
        api_version=g.api_version,
        job_options=job_options,
    )
    job_id = job_info.id
    response = make_response("", 201)
    response.headers['Location'] = url_for('.get_job_info', job_id=job_id)
    response.headers['OpenEO-Identifier'] = str(job_id)
    return response


@api_endpoint
@openeo_bp.route('/jobs', methods=['GET'])
@auth_handler.requires_bearer_auth
def list_jobs(user: User):
    return jsonify({
        "jobs": [
            _jsonable_batch_job_metadata(m, full=False)
            for m in backend_implementation.batch_jobs.get_user_jobs(user.user_id)
        ],
        "links": [],
    })


def _jsonable_batch_job_metadata(metadata: BatchJobMetadata, full=True) -> dict:
    """API-version-aware conversion of service metadata to jsonable dict"""
    d = metadata.prepare_for_json()
    # Fields to export
    fields = ['id', 'title', 'description', 'status', 'created', 'updated', 'plan', 'costs', 'budget']
    if full:
        fields.extend(['process', 'progress'])
    d = {k: v for (k, v) in d.items() if k in fields}

    if requested_api_version().below("1.0.0"):
        d["process_graph"] = d.pop("process", {}).get("process_graph")
        d["submitted"] = d.pop("created", None)
        # TODO wider status checking coverage?
        if d["status"] == "created":
            d["status"] = "submitted"

    return dict_no_none(**d)


@api_endpoint
@openeo_bp.route('/jobs/<job_id>', methods=['GET'])
@auth_handler.requires_bearer_auth
def get_job_info(job_id, user: User):
    job_info = backend_implementation.batch_jobs.get_job_info(job_id, user.user_id)
    return jsonify(_jsonable_batch_job_metadata(job_info))


@api_endpoint
@openeo_bp.route('/jobs/<job_id>' , methods=['DELETE'])
@auth_handler.requires_bearer_auth
def delete_job(job_id, user: User):
    raise NotImplementedError


@api_endpoint
@openeo_bp.route('/jobs/<job_id>' , methods=['PATCH'])
@auth_handler.requires_bearer_auth
def modify_job(job_id, user: User):
    raise NotImplementedError


@api_endpoint
@openeo_bp.route('/jobs/<job_id>/results', methods=['POST'])
@auth_handler.requires_bearer_auth
def queue_job(job_id, user: User):
    backend_implementation.batch_jobs.start_job(job_id=job_id, user_id=user.user_id)
    return make_response("", 202)


@api_endpoint
@openeo_bp.route('/jobs/<job_id>/results', methods=['GET'])
@auth_handler.requires_bearer_auth
def list_job_results(job_id, user: User):
    # TODO: error JobNotFinished when job is not finished yet
    results = backend_implementation.batch_jobs.get_results(job_id=job_id, user_id=user.user_id)
    filenames = results.keys()
    if requested_api_version().at_least("1.0.0"):
        result = {
            # TODO: #EP-3281 API 1.0 required fields: "stac_version", "id", "type", "bbox", "geometry", "properties", "assets"
            "assets": {
                filename: {
                    "href": url_for('.download_job_result', job_id=job_id, filename=filename, _external=True)
                    # TODO #EP-3281 add "type" field with media type
                }
                for filename in filenames
            }
        }
    else:
        result = {
            "links": [
                {"href": url_for('.download_job_result', job_id=job_id, filename=filename, _external=True)}
                for filename in filenames
            ]
        }

    # TODO "OpenEO-Costs" header?
    return jsonify(result)


@api_endpoint
@openeo_bp.route('/jobs/<job_id>/results/<filename>', methods=['GET'])
@auth_handler.requires_bearer_auth
def download_job_result(job_id, filename, user: User):
    results = backend_implementation.batch_jobs.get_results(job_id=job_id, user_id=user.user_id)
    if filename not in results:
        raise FilePathInvalidException
    output_dir = results[filename]
    return send_from_directory(output_dir, filename)


@api_endpoint
@openeo_bp.route('/jobs/<job_id>/logs', methods=['GET'])
@auth_handler.requires_bearer_auth
def get_job_logs(job_id, user: User):
    offset = request.args.get('offset', 0)
    return jsonify({
        "logs": backend_implementation.batch_jobs.get_log_entries(job_id=job_id, user_id=user.user_id, offset=offset),
        "links": [],
    })


@api_endpoint
@openeo_bp.route('/jobs/<job_id>/results', methods=['DELETE'])
@auth_handler.requires_bearer_auth
def cancel_job(job_id, user: User):
    backend_implementation.batch_jobs.cancel_job(job_id=job_id, user_id=user.user_id)
    return make_response("", 204)

#SERVICES API https://open-eo.github.io/openeo-api/v/0.3.0/apireference/#tag/Web-Service-Management


@api_endpoint
@openeo_bp.route('/service_types', methods=['GET'])
def service_types():
    service_types = backend_implementation.secondary_services.service_types()
    expected_fields = {"configuration", "process_parameters", "links"}
    assert all(set(st.keys()) == expected_fields for st in service_types.values())
    if requested_api_version().below("1.0.0"):
        # Old style response  (https://github.com/Open-EO/openeo-api/issues/161)
        service_types = {
            name: {"parameters": st["configuration"], "attributes": [],
                   "variables": st["process_parameters"], "links": st["links"]}
            for name, st in service_types.items()
        }
    return jsonify(service_types)


@api_endpoint
@openeo_bp.route('/services', methods=['POST'])
def services_post():
    """
    Create a secondary web service such as WMTS, TMS or WCS. The underlying data is processes on-demand, but a process graph may simply access results from a batch job. Computations should be performed in the sense that it is only evaluated for the requested spatial / temporal extent and resolution.

    Note: Costs incurred by shared secondary web services are usually paid by the owner, but this depends on the service type and whether it supports charging fees or not.
    https://open-eo.github.io/openeo-api/v/0.3.0/apireference/#tag/Secondary-Services-Management/paths/~1services/post

    :return:
    """
    # TODO require authenticated user
    post_data = request.get_json()
    service_metadata = backend_implementation.secondary_services.create_service(
        process_graph=_extract_process_graph(post_data),
        service_type=post_data["type"],
        api_version=g.api_version,
        post_data=post_data,
    )

    return make_response('', 201, {
        'Content-Type': 'application/json',
        'Location': url_for('.get_service_info', service_id=service_metadata.id),
        'OpenEO-Identifier': service_metadata.id,
    })


def _jsonable_service_metadata(metadata: ServiceMetadata, full=True) -> dict:
    """API-version-aware conversion of service metadata to jsonable dict"""
    d = metadata.prepare_for_json()
    if not full:
        d.pop("process")
        d.pop("attributes")
    if requested_api_version().below("1.0.0"):
        d["process_graph"] = d.pop("process", {}).get("process_graph")
        d["parameters"] = d.pop("configuration", None) or ({} if full else None)
        d["submitted"] = d.pop("created", None)
    return dict_no_none(**d)


@api_endpoint
@openeo_bp.route('/services', methods=['GET'])
def services_get():
    """List all running secondary web services for authenticated user"""
    # TODO Require authentication
    return jsonify({
        "services": [
            _jsonable_service_metadata(m, full=False)
            for m in backend_implementation.secondary_services.list_services()
        ],
        "links": [],
    })


@api_endpoint
@openeo_bp.route('/services/<service_id>', methods=['GET'])
def get_service_info(service_id):
    # TODO Require authentication
    try:
        metadata = backend_implementation.secondary_services.service_info(service_id)
    except Exception:
        raise ServiceNotFoundException(service_id)
    return jsonify(_jsonable_service_metadata(metadata, full=True))


@api_endpoint
@openeo_bp.route('/services/<service_id>', methods=['PATCH'])
def service_patch(service_id):
    # TODO Require authentication
    process_graph = _extract_process_graph(request.get_json())
    backend_implementation.secondary_services.update_service(service_id, process_graph=process_graph)
    return response_204_no_content()


@api_endpoint
@openeo_bp.route('/services/<service_id>', methods=['DELETE'])
def service_delete(service_id):
    # TODO Require authentication
    backend_implementation.secondary_services.remove_service(service_id)
    return response_204_no_content()


@api_endpoint
@openeo_bp.route('/subscription', methods=["GET"])
def subscription():
    raise NotImplementedError


def _normalize_collection_metadata(metadata: dict, api_version: ComparableVersion, full=False) -> dict:
    """
    Make sure the given collection metadata roughly complies to desirec version of OpenEO spec.
    """
    # Make copy and remove all "private" fields
    metadata = copy.deepcopy(metadata)
    metadata = {k: v for (k, v) in metadata.items() if not k.startswith('_')}

    # Metadata should at least contain an id.
    if "id" not in metadata:
        _log.error("Collection metadata should have 'id' field: {m!r}".format(m=metadata))
        raise KeyError("id")
    collection_id = metadata["id"]

    # Version dependent metadata conversions
    cube_dims_100 = deep_get(metadata, "cube:dimensions", default=None)
    cube_dims_040 = deep_get(metadata, "properties", "cube:dimensions", default=None)
    eo_bands_100 = deep_get(metadata, "summaries", "eo:bands", default=None)
    eo_bands_040 = deep_get(metadata, "properties", "eo:bands", default=None)
    if api_version.below("1.0.0"):
        if full and not cube_dims_040 and cube_dims_100:
            metadata.setdefault("properties", {})
            metadata["properties"]["cube:dimensions"] = cube_dims_100
        if full and not eo_bands_040 and eo_bands_100:
            metadata.setdefault("properties", {})
            metadata["properties"]["eo:bands"] = eo_bands_100
    else:
        if full and not cube_dims_100 and cube_dims_040:
            _log.warning("Collection metadata 'cube:dimensions' in API 0.4 style instead of 1.0 style")
            metadata["cube:dimensions"] = cube_dims_040
        if full and not eo_bands_100 and eo_bands_040:
            _log.warning("Collection metadata 'eo:bands' in API 0.4 style instead of 1.0 style")
            metadata.setdefault("summaries", {})
            metadata["summaries"]["eo:bands"] = eo_bands_040

    # Make sure some required fields are set.
    metadata.setdefault("stac_version", "0.9.0" if api_version.at_least("1.0.0") else "0.6.2")
    metadata.setdefault("links", [])
    metadata.setdefault("description", collection_id)
    metadata.setdefault("license", "proprietary")
    # Warn about missing fields where simple defaults are not feasible.
    fallbacks = {
        "extent": {"spatial": [0, 0, 0, 0], "temporal": [None, None]},
    }
    if full:
        if api_version.at_least("1.0.0"):
            fallbacks["cube:dimensions"] = {}
            fallbacks["summaries"] = {}
        else:
            fallbacks["properties"] = {}
            fallbacks["other_properties"] = {}

    for key, value in fallbacks.items():
        if key not in metadata:
            _log.warning("Collection {c!r} metadata does not have field {k!r}.".format(c=collection_id, k=key))
            metadata[key] = value

    if not full:
        basic_keys = [
            "stac_version", "stac_extensions", "id", "title", "description", "keywords", "version",
            "deprecated", "license", "providers", "extent", "links"
        ]
        metadata = {k: v for k, v in metadata.items() if k in basic_keys}

    return metadata


@api_endpoint
@openeo_bp.route('/collections', methods=['GET'])
def collections():
    metadata = [
        _normalize_collection_metadata(metadata=m, api_version=requested_api_version(), full=False)
        for m in backend_implementation.catalog.get_all_metadata()
    ]
    return jsonify({
        'collections': metadata,
        'links': []
    })


@api_endpoint
@openeo_bp.route('/collections/<collection_id>', methods=['GET'])
def collection_by_id(collection_id):
    metadata = backend_implementation.catalog.get_collection_metadata(collection_id=collection_id)
    metadata = _normalize_collection_metadata(metadata=metadata, api_version=requested_api_version(), full=True)
    return jsonify(metadata)


@api_endpoint
@openeo_bp.route('/processes', methods=['GET'])
def processes():
    # TODO: this `qname` feature is non-standard. Is this necessary for some reason?
    substring = request.args.get('qname')
    processes = get_process_registry(requested_api_version()).get_specs(substring)
    return jsonify({'processes': processes, 'links': []})


@api_endpoint
@openeo_bp.route('/processes/<process_id>', methods=['GET'])
def process(process_id):
    spec = get_process_registry(requested_api_version()).get_spec(name=process_id)
    return jsonify(spec)


app.register_blueprint(openeo_bp, url_prefix='/openeo')
app.register_blueprint(openeo_bp, url_prefix='/openeo/<version>')

# Build endpoint metadata dictionary
_openeo_endpoint_metadata = api_endpoint.get_path_metadata(openeo_bp)


# Note: /.well-known/openeo should be available directly under domain, without version prefix.
@app.route('/.well-known/openeo', methods=['GET'])
def well_known_openeo():
    return jsonify({
        'versions': [
            {
                "url": url_for('openeo.index', version=k, _external=True),
                "api_version": v.version,
                "production": v.production,
            }
            for k, v in API_VERSIONS.items()
            if v.advertised
        ]
    })
