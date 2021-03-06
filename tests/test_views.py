from contextlib import contextmanager
from datetime import datetime
import logging
import os
from pathlib import Path
import re
from unittest import TestCase, mock

import flask
from flask.testing import FlaskClient
import pytest

from openeo.capabilities import ComparableVersion
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.dummy import dummy_backend
import openeo_driver.testing
from openeo_driver.testing import TEST_USER, ApiResponse
from openeo_driver.users import HttpAuthHandler
from openeo_driver.views import app, EndpointRegistry, build_backend_deploy_metadata, _normalize_collection_metadata
from .data import TEST_DATA_ROOT
from .test_users import _build_basic_auth_header

os.environ["DRIVER_IMPLEMENTATION_PACKAGE"] = "openeo_driver.dummy.dummy_backend"
app.config["OPENEO_TITLE"] = "OpenEO Test API"


@pytest.fixture(params=["0.4.0", "1.0.0"])
def api_version(request):
    return request.param


@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeo.net'
    return app.test_client()


class ApiTester(openeo_driver.testing.ApiTester):
    """Helper container class for compact writing of api version aware `views` tests"""

    def __init__(self, api_version: str, client: FlaskClient):
        super().__init__(api_version=api_version, client=client, data_root=TEST_DATA_ROOT)


@pytest.fixture
def api(api_version, client) -> ApiTester:
    return ApiTester(api_version=api_version, client=client)


@pytest.fixture
def api040(client) -> ApiTester:
    return ApiTester(api_version="0.4.0", client=client)


@pytest.fixture
def api100(client) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=client)


class TestGeneral:
    """
    General tests (capabilities, collections, processes)
    """

    def test_well_known_openeo(self, client):
        resp = client.get('/.well-known/openeo')
        assert resp.status_code == 200
        for expected in [
            {'api_version': '0.4.0', 'production': True, 'url': 'http://oeo.net/openeo/0.4.0/'},
            {'api_version': '0.4.2', 'production': True, 'url': 'http://oeo.net/openeo/0.4.2/'},
            {'api_version': '0.4.2', 'production': True, 'url': 'http://oeo.net/openeo/0.4/'},
            {'api_version': '1.0.0', 'production': False, 'url': 'http://oeo.net/openeo/1.0.0/'},
        ]:
            assert expected in resp.json["versions"]

    def test_capabilities_040(self, api040):
        capabilities = api040.get('/').assert_status_code(200).json
        assert capabilities["api_version"] == "0.4.0"
        assert capabilities["version"] == "0.4.0"
        assert capabilities["stac_version"] == "0.9.0"
        assert capabilities["title"] == "OpenEO Test API"
        assert capabilities["id"] == "openeotestapi-0.4.0"
        assert capabilities["production"] is True

    def test_capabilities_100(self, api100):
        capabilities = api100.get('/').assert_status_code(200).json
        assert capabilities["api_version"] == "1.0.0"
        assert capabilities["stac_version"] == "0.9.0"
        assert capabilities["title"] == "OpenEO Test API"
        assert capabilities["id"] == "openeotestapi-1.0.0"
        assert capabilities["production"] is False

    def test_capabilities_version_alias(self, client):
        resp = ApiResponse(client.get('/openeo/0.4/')).assert_status_code(200).json
        assert resp["api_version"] == "0.4.2"
        assert resp["production"] == True

    def test_capabilities_invalid_api_version(self, client):
        resp = ApiResponse(client.get('/openeo/0.0.0/'))
        resp.assert_error(501, 'UnsupportedApiVersion')
        assert "Unsupported version: '0.0.0'" in resp.json['message']

    def test_capabilities_endpoints(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"]: sorted(e["methods"]) for e in capabilities["endpoints"]}
        assert endpoints["/collections"] == ["GET"]
        assert endpoints["/collections/{collection_id}"] == ["GET"]
        assert endpoints["/result"] == ["POST"]
        assert endpoints["/jobs"] == ["GET", "POST"]
        assert endpoints["/processes"] == ["GET"]
        assert endpoints["/processes/{process_id}"] == ["GET"]
        assert endpoints["/udf_runtimes"] == ["GET"]
        assert endpoints["/file_formats"] == ["GET"]
        assert endpoints["/service_types"] == ["GET"]
        assert endpoints["/services"] == ["GET", "POST"]
        assert endpoints["/services/{service_id}"] == ["DELETE", "GET", "PATCH"]
        assert endpoints["/subscription"] == ["GET"]
        assert endpoints["/jobs/{job_id}"] == ["DELETE", "GET", "PATCH"]
        assert endpoints["/jobs/{job_id}/results"] == ["DELETE", "GET", "POST"]
        assert endpoints["/credentials/basic"] == ["GET"]
        assert endpoints["/credentials/oidc"] == ["GET"]
        assert endpoints["/me"] == ["GET"]

    def test_capabilities_endpoints_issue_28_v040(self, api040):
        """https://github.com/Open-EO/openeo-python-driver/issues/28"""
        capabilities = api040.get("/").assert_status_code(200).json
        endpoints = {e["path"]: e["methods"] for e in capabilities["endpoints"]}
        assert endpoints["/output_formats"] == ["GET"]
        assert "/file_formats" not in endpoints

    def test_capabilities_endpoints_issue_28_v100(self, api100):
        """https://github.com/Open-EO/openeo-python-driver/issues/28"""
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"]: e["methods"] for e in capabilities["endpoints"]}
        assert endpoints["/file_formats"] == ["GET"]
        assert "/output_formats" not in endpoints

    def test_health(self, api):
        resp = api.get('/health').assert_status_code(200).json
        assert resp == {"health": "OK"}

    def test_output_formats(self, api040):
        resp = api040.get('/output_formats').assert_status_code(200).json
        assert resp == {"GTiff": {"title": "GeoTiff", "gis_data_types": ["raster"]}, }

    def test_file_formats(self, api100):
        resp = api100.get('/file_formats').assert_status_code(200).json
        assert resp == {
            "input": {"GeoJSON": {"gis_data_type": ["vector"]}},
            "output": {
                "GTiff": {"title": "GeoTiff", "gis_data_types": ["raster"]},
            }
        }

    def test_processes(self, api):
        resp = api.get('/processes').assert_status_code(200).json
        processes = resp["processes"]
        process_ids = set(p['id'] for p in processes)
        assert {"load_collection", "min", "max", "sin", "merge_cubes", "mask"}.issubset(process_ids)
        expected_keys = {"id", "description", "parameters", "returns"}
        for process in processes:
            assert all(k in process for k in expected_keys)

    def test_processes_non_standard_histogram(self, api):
        resp = api.get('/processes').assert_status_code(200).json
        histogram_spec, = [p for p in resp["processes"] if p['id'] == "histogram"]
        assert "into bins" in histogram_spec["description"]
        if api.api_version_compare.at_least("1.0.0"):
            assert histogram_spec["parameters"] == [
                {
                    'name': 'data',
                    'description': 'An array of numbers',
                    'optional': False,
                    'schema': {'type': 'array', 'items': {'type': ['number', 'null']}, }
                }
            ]
        else:
            assert histogram_spec["parameters"] == {
                'data': {
                    'description': 'An array of numbers',
                    'required': True,
                    'schema': {'type': 'array', 'items': {'type': ['number', 'null']}, }
                }
            }
        assert histogram_spec["returns"] == {
            'description': 'A sequence of (bin, count) pairs',
            'schema': {'type': 'object'}
        }

    def test_process_details(self, api):
        spec = api.get('/processes/sin').assert_status_code(200).json
        assert spec['id'] == 'sin'
        assert "Computes the sine" in spec['description']
        if api.api_version_compare.at_least("1.0.0"):
            assert spec["parameters"] == [
                {'name': 'x', 'description': 'An angle in radians.', 'schema': {'type': ['number', 'null']}}
            ]
        else:
            assert spec["parameters"] == {
                'x': {
                    'description': 'An angle in radians.', 'required': True, 'schema': {'type': ['number', 'null']}
                }
            }
        assert spec["returns"]["schema"] == {'type': ['number', 'null']}

    def test_process_details_invalid(self, api):
        api.get('/processes/blergh').assert_error(400, 'ProcessUnsupported')

    def test_processes_040_vs_100(self, api040, api100):
        pids040 = {p['id'] for p in api040.get("/processes").assert_status_code(200).json["processes"]}
        pids100 = {p['id'] for p in api100.get("/processes").assert_status_code(200).json["processes"]}
        expected_only_040 = {'reduce', 'aggregate_polygon'}
        expected_only_100 = {'reduce_dimension', 'aggregate_spatial', 'mask_polygon', 'add'}
        for pid in expected_only_040:
            assert pid in pids040
            assert pid not in pids100
        for pid in expected_only_100:
            assert pid not in pids040
            assert pid in pids100


class TestCollections:

    def test_normalize_collection_metadata_no_id(self, caplog):
        with pytest.raises(KeyError):
            _normalize_collection_metadata({"foo": "bar"}, api_version=ComparableVersion("1.0.0"))
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any("should have 'id' field" in m for m in errors)

    def test_normalize_collection_metadata_minimal_040(self, caplog):
        assert _normalize_collection_metadata({"id": "foobar"}, api_version=ComparableVersion("0.4.2")) == {
            'id': 'foobar',
            'stac_version': '0.6.2',
            'description': 'foobar',
            'extent': {'spatial': [0, 0, 0, 0], 'temporal': [None, None]},
            'license': 'proprietary',
            'links': [],
        }
        warnings = set(r.getMessage() for r in caplog.records if r.levelno == logging.WARN)
        assert warnings == {"Collection 'foobar' metadata does not have field 'extent'."}

    def test_normalize_collection_metadata_minimal_full_040(self, caplog):
        assert _normalize_collection_metadata({"id": "foobar"}, api_version=ComparableVersion("0.4.2"), full=True) == {
            'id': 'foobar',
            'stac_version': '0.6.2',
            'description': 'foobar',
            'extent': {'spatial': [0, 0, 0, 0], 'temporal': [None, None]},
            'license': 'proprietary',
            'properties': {},
            'other_properties': {},
            'links': [],
        }
        warnings = set(r.getMessage() for r in caplog.records if r.levelno == logging.WARN)
        assert warnings == {
            "Collection 'foobar' metadata does not have field 'extent'.",
            "Collection 'foobar' metadata does not have field 'other_properties'.",
            "Collection 'foobar' metadata does not have field 'properties'.",
        }

    def test_normalize_collection_metadata_minimal_100(self, caplog):
        assert _normalize_collection_metadata({"id": "foobar"}, api_version=ComparableVersion("1.0.0")) == {
            'id': 'foobar',
            'stac_version': '0.9.0',
            'description': 'foobar',
            'extent': {'spatial': [0, 0, 0, 0], 'temporal': [None, None]},
            'license': 'proprietary',
            'links': [],
        }
        warnings = set(r.getMessage() for r in caplog.records if r.levelno == logging.WARN)
        assert warnings == {"Collection 'foobar' metadata does not have field 'extent'."}

    def test_normalize_collection_metadata_minimal_full_100(self, caplog):
        assert _normalize_collection_metadata({"id": "foobar"}, api_version=ComparableVersion("1.0.0"), full=True) == {
            'id': 'foobar',
            'stac_version': '0.9.0',
            'description': 'foobar',
            'extent': {'spatial': [0, 0, 0, 0], 'temporal': [None, None]},
            'license': 'proprietary',
            'cube:dimensions': {},
            'summaries': {},
            'links': [],
        }
        warnings = set(r.getMessage() for r in caplog.records if r.levelno == logging.WARN)
        assert warnings == {
            "Collection 'foobar' metadata does not have field 'cube:dimensions'.",
            "Collection 'foobar' metadata does not have field 'extent'.",
            "Collection 'foobar' metadata does not have field 'summaries'."
        }

    def test_normalize_collection_metadata_dimensions_and_bands_040(self, caplog):
        metadata = {
            "id": "foobar",
            "cube:dimensions": {
                "x": {"type": "spatial"},
                "b": {"type": "bands", "values": ["B02", "B03"]}
            },
            "summaries": {
                "eo:bands": [{"name": "B02"}, {"name": "B03"}]
            }
        }
        res = _normalize_collection_metadata(metadata, api_version=ComparableVersion("0.4.0"), full=True)
        assert res["properties"]["cube:dimensions"] == {
            "x": {"type": "spatial"},
            "b": {"type": "bands", "values": ["B02", "B03"]}
        }
        assert res["properties"]["eo:bands"] == [{"name": "B02"}, {"name": "B03"}]

    def test_normalize_collection_metadata_dimensions_and_bands_100(self, caplog):
        metadata = {
            "id": "foobar",
            "properties": {
                "cube:dimensions": {
                    "x": {"type": "spatial"},
                    "b": {"type": "bands", "values": ["B02", "B03"]}
                },
                "eo:bands": [{"name": "B02"}, {"name": "B03"}]
            }
        }
        res = _normalize_collection_metadata(metadata, api_version=ComparableVersion("1.0.0"), full=True)
        assert res["cube:dimensions"] == {
            "x": {"type": "spatial"},
            "b": {"type": "bands", "values": ["B02", "B03"]}
        }
        assert res["summaries"]["eo:bands"] == [{"name": "B02"}, {"name": "B03"}]

    def test_collections(self, api):
        resp = api.get('/collections').assert_status_code(200).json
        assert "links" in resp
        assert "collections" in resp
        assert 'S2_FAPAR_CLOUDCOVER' in [c['id'] for c in resp['collections']]
        assert 'S2_FOOBAR' in [c['id'] for c in resp['collections']]
        for collection in resp['collections']:
            assert 'id' in collection
            assert 'stac_version' in collection
            assert 'description' in collection
            assert 'license' in collection
            assert 'extent' in collection
            assert 'links' in collection

    def test_strip_private_fields(self, api):
        assert '_private' in dummy_backend.DummyCatalog().get_collection_metadata("S2_FOOBAR")
        # All metadata
        collections = api.get('/collections').assert_status_code(200).json["collections"]
        metadata, = (c for c in collections if c["id"] == "S2_FOOBAR")
        assert '_private' not in metadata
        # Single collection metadata
        metadata = api.get('/collections/S2_FOOBAR').assert_status_code(200).json
        assert '_private' not in metadata

    def test_collections_detail_invalid_collection(self, api):
        error = api.get('/collections/FOOBOO').assert_error(404, "CollectionNotFound").json
        assert error["message"] == "Collection 'FOOBOO' does not exist."

    def test_collections_detail(self, api):
        collection = api.get('/collections/S2_FOOBAR').assert_status_code(200).json
        assert collection['id'] == 'S2_FOOBAR'
        assert collection['description'] == 'S2_FOOBAR'
        assert collection['license'] == 'free'
        assert collection['extent'] == {'spatial': [2.5, 49.5, 6.2, 51.5], 'temporal': ['2019-01-01', None]}
        cube_dimensions = {
            "x": {"type": "spatial"}, "y": {"type": "spatial"}, "t": {"type": "temporal"},
            "bands": {"type": "bands", "values": ["B02", "B03", "B04", "B08"]},
        }
        eo_bands = [
            {"name": "B02", "common_name": "blue"}, {"name": "B03", "common_name": "green"},
            {"name": "B04", "common_name": "red"}, {"name": "B08", "common_name": "nir"},
        ]
        if api.api_version_compare.at_least("1.0.0"):
            assert collection['stac_version'] == '0.9.0'
            assert collection['cube:dimensions'] == cube_dimensions
            assert collection['summaries']['eo:bands'] == eo_bands
        else:
            assert collection['stac_version'] == '0.6.2'
            assert collection['properties']['cube:dimensions'] == cube_dimensions
            assert collection['properties']["eo:bands"] == eo_bands


class TestBatchJobs:
    AUTH_HEADER = {
        "Authorization": "Bearer " + HttpAuthHandler().build_basic_access_token(user_id=TEST_USER)
    }

    @staticmethod
    @contextmanager
    def _fresh_job_registry(next_job_id):
        """Set up a fresh job registry and predefine next job id"""
        with mock.patch.object(dummy_backend.DummyBatchJobs, 'generate_job_id', return_value=next_job_id):
            dummy_backend.DummyBatchJobs._job_registry = {
                (TEST_USER, '07024ee9-7847-4b8a-b260-6c879a2b3cdc'): BatchJobMetadata(
                    id='07024ee9-7847-4b8a-b260-6c879a2b3cdc',
                    status='running',
                    process={'process_graph': {'foo': {'process_id': 'foo', 'arguments': {}}}},
                    created=datetime(2017, 1, 1, 9, 32, 12),
                )
            }
            yield

    def test_create_job_040(self, api040):
        with self._fresh_job_registry(next_job_id="job-220"):
            resp = api040.post('/jobs', headers=self.AUTH_HEADER, json={
                'title': 'foo job',
                'process_graph': {"foo": {"process_id": "foo", "arguments": {}}},
            }).assert_status_code(201)
        assert resp.headers['Location'] == 'http://oeo.net/openeo/0.4.0/jobs/job-220'
        assert resp.headers['OpenEO-Identifier'] == 'job-220'
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, 'job-220']
        assert job_info.id == "job-220"
        assert job_info.process == {"process_graph": {"foo": {"process_id": "foo", "arguments": {}}}}
        assert job_info.status == "created"
        assert job_info.created == dummy_backend.DEFAULT_DATETIME
        assert job_info.job_options is None

    def test_create_job_with_options_040(self, api040):
        with self._fresh_job_registry(next_job_id="job-230"):
            resp = api040.post('/jobs', headers=self.AUTH_HEADER, json={
                'title': 'foo job',
                'process_graph': {"foo": {"process_id": "foo", "arguments": {}}},
                'job_options': {"driver-memory": "3g", "executor-memory": "5g"},
            }).assert_status_code(201)
        assert resp.headers['Location'] == 'http://oeo.net/openeo/0.4.0/jobs/job-230'
        assert resp.headers['OpenEO-Identifier'] == 'job-230'
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, 'job-230']
        assert job_info.job_options == {"driver-memory": "3g", "executor-memory": "5g"}

    def test_create_job_100(self, api100):
        with self._fresh_job_registry(next_job_id="job-245"):
            resp = api100.post('/jobs', headers=self.AUTH_HEADER, json={
                'process': {
                    'process_graph': {"foo": {"process_id": "foo", "arguments": {}}},
                    'summary': 'my foo job',
                },
            }).assert_status_code(201)
        assert resp.headers['Location'] == 'http://oeo.net/openeo/1.0.0/jobs/job-245'
        assert resp.headers['OpenEO-Identifier'] == 'job-245'
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, 'job-245']
        assert job_info.id == "job-245"
        assert job_info.process == {"process_graph": {"foo": {"process_id": "foo", "arguments": {}}}}
        assert job_info.status == "created"
        assert job_info.created == dummy_backend.DEFAULT_DATETIME
        assert job_info.job_options is None

    def test_create_job_100_with_options(self, api100):
        with self._fresh_job_registry(next_job_id="job-256"):
            resp = api100.post('/jobs', headers=self.AUTH_HEADER, json={
                'process': {
                    'process_graph': {"foo": {"process_id": "foo", "arguments": {}}},
                    'summary': 'my foo job',
                },
                'job_options': {"driver-memory": "3g", "executor-memory": "5g"},
            }).assert_status_code(201)
        assert resp.headers['Location'] == 'http://oeo.net/openeo/1.0.0/jobs/job-256'
        assert resp.headers['OpenEO-Identifier'] == 'job-256'
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, 'job-256']
        assert job_info.job_options == {"driver-memory": "3g", "executor-memory": "5g"}

    def test_start_job(self, api):
        with self._fresh_job_registry(next_job_id="job-267"):
            api.post('/jobs', headers=self.AUTH_HEADER, json=api.get_process_graph_dict(
                {"foo": {"process_id": "foo", "arguments": {}}},
            )).assert_status_code(201)
            assert dummy_backend.DummyBatchJobs._job_registry[TEST_USER, 'job-267'].status == "created"
            api.post('/jobs/job-267/results', headers=self.AUTH_HEADER, json={}).assert_status_code(202)
            assert dummy_backend.DummyBatchJobs._job_registry[TEST_USER, 'job-267'].status == "running"

    def test_start_job_invalid(self, api):
        resp = api.post('/jobs/deadbeef-f00/results', headers=self.AUTH_HEADER)
        resp.assert_error(404, "JobNotFound")
        assert resp.json["message"] == "The job 'deadbeef-f00' does not exist."

    def test_get_job_info_040(self, api040):
        resp = api040.get('/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc', headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            'id': '07024ee9-7847-4b8a-b260-6c879a2b3cdc',
            'status': 'running',
            'submitted': "2017-01-01T09:32:12Z",
            'process_graph': {'foo': {'process_id': 'foo', 'arguments': {}}},
        }

    def test_get_job_info_100(self, api100):
        resp = api100.get('/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc', headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            'id': '07024ee9-7847-4b8a-b260-6c879a2b3cdc',
            'status': 'running',
            'created': "2017-01-01T09:32:12Z",
            'process': {'process_graph': {'foo': {'process_id': 'foo', 'arguments': {}}}},
        }

    def test_get_job_info_invalid(self, api):
        resp = api.get('/jobs/deadbeef-f00', headers=self.AUTH_HEADER).assert_error(404, "JobNotFound")
        assert resp.json["message"] == "The job 'deadbeef-f00' does not exist."

    def test_list_user_jobs_040(self, api040):
        with self._fresh_job_registry(next_job_id="job-318"):
            resp = api040.get('/jobs', headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            "jobs": [
                {
                    'id': '07024ee9-7847-4b8a-b260-6c879a2b3cdc',
                    'status': 'running',
                    'submitted': "2017-01-01T09:32:12Z",
                }
            ],
            "links": []
        }

    def test_list_user_jobs_100(self, api100):
        with self._fresh_job_registry(next_job_id="job-332"):
            resp = api100.get('/jobs', headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            "jobs": [
                {
                    'id': '07024ee9-7847-4b8a-b260-6c879a2b3cdc',
                    'status': 'running',
                    'created': "2017-01-01T09:32:12Z",
                }
            ],
            "links": []
        }

    def test_get_job_results_unfinished(self, api):
        with self._fresh_job_registry(next_job_id="job-345"):
            resp = api.get('/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results', headers=self.AUTH_HEADER)
        resp.assert_error(400, "JobNotFinished")

    def test_get_job_results_040(self, api040):
        with self._fresh_job_registry(next_job_id="job-349"):
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc", user_id=TEST_USER, status="finished")
            resp = api040.get('/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results', headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            "links": [
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/output.tiff"
                }
            ]
        }

    def test_get_job_results_100(self, api100):
        with self._fresh_job_registry(next_job_id="job-362"):
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc", user_id=TEST_USER, status="finished")
            resp = api100.get('/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results', headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            "assets": {
                "output.tiff": {
                    "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/output.tiff"
                }
            }
        }

    def test_get_job_results_invalid_job(self, api):
        api.get('/jobs/deadbeef-f00/results', headers=self.AUTH_HEADER).assert_error(404, "JobNotFound")

    def test_download_result_invalid_job(self, api):
        api.get('/jobs/deadbeef-f00/results/some_file', headers=self.AUTH_HEADER).assert_error(404, "JobNotFound")

    def test_download_result(self, api, tmp_path):
        output_root = Path(tmp_path)
        with mock.patch.object(dummy_backend.DummyBatchJobs, '_output_root', return_value=output_root):
            output = output_root / "07024ee9-7847-4b8a-b260-6c879a2b3cdc" / "out" / "output.tiff"
            output.parent.mkdir(parents=True)
            with output.open("wb") as f:
                f.write(b"tiffdata")
            resp = api.get("/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/output.tiff", headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).data == b"tiffdata"

    def test_get_batch_job_logs(self, api):
        resp = api.get('/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/logs', headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            "logs": [
                {"id": "1", "level": "info", "message": "hello world", "path": []}
            ],
            "links": []
        }

    def test_cancel_job(self, api):
        with self._fresh_job_registry(next_job_id="job-403"):
            resp = api.delete('/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results', headers=self.AUTH_HEADER)
        assert resp.status_code == 204

    def test_cancel_job_invalid(self, api):
        with self._fresh_job_registry(next_job_id="job-403"):
            resp = api.delete('/jobs/deadbeef-f00/results', headers=self.AUTH_HEADER)
        resp.assert_error(404, "JobNotFound")


class TestSecondaryServices(TestCase):
    # TODO: port to pytest style fixtures instead of TestCase.setUp

    def setUp(self):
        app.config['TESTING'] = True
        app.config['SERVER_NAME'] = 'oeo.net'
        self.client = app.test_client()
        self._auth_header = {
            "Authorization": "Bearer " + HttpAuthHandler().build_basic_access_token(user_id=TEST_USER)
        }
        dummy_backend.collections = {}

    def test_service_types_v040(self):
        resp = self.client.get('/openeo/0.4.0/service_types')
        service_types = resp.json
        assert list(service_types.keys()) == ["WMTS"]
        wmts = service_types["WMTS"]
        assert wmts["parameters"]["version"]["default"] == "1.0.0"
        assert wmts["variables"] == []
        assert wmts["attributes"] == []

    def test_service_types_v100(self):
        resp = self.client.get('/openeo/1.0.0/service_types')
        service_types = resp.json
        assert list(service_types.keys()) == ["WMTS"]
        wmts = service_types["WMTS"]
        assert wmts["configuration"]["version"]["default"] == "1.0.0"
        assert wmts["process_parameters"] == []
        assert wmts["links"] == []

    def test_create_unsupported_service_type_returns_BadRequest(self):
        resp = self.client.post('/openeo/services', content_type='application/json', json={
            "process_graph": {'product_id': 'S2'},
            "type": '???',
        })

        self.assertEqual(400, resp.status_code)

    def test_unsupported_services_methods_return_MethodNotAllowed(self):
        resp = self.client.put('/openeo/services', content_type='application/json', json={
            "process_graph": {'product_id': 'S2'},
            "type": 'WMTS',
        })

        self.assertEqual(405, resp.status_code)

    def test_uncaught_exceptions_return_InternalServerError(self):
        resp = self.client.post('/openeo/services', content_type='application/json', json={
            "process_graph": {'product_id': 'S2'}
        })

        self.assertEqual(500, resp.status_code)

    def test_list_services_040(self):
        metadata = self.client.get('/openeo/0.4.0/services').json
        assert metadata == {
            "services": [{
                'id': 'wmts-foo',
                'type': 'WMTS',
                'enabled': True,
                'url': 'https://oeo.net/wmts/foo',
                'submitted': '2020-04-09T15:05:08Z',
                'title': 'Test service',
            }],
            "links": []
        }

    def test_list_services_100(self):
        metadata = self.client.get('/openeo/1.0.0/services').json
        assert metadata == {
            "services": [{
                'id': 'wmts-foo',
                'type': 'WMTS',
                'enabled': True,
                'url': 'https://oeo.net/wmts/foo',
                'title': 'Test service',
                'created': '2020-04-09T15:05:08Z',
            }],
            "links": []
        }

    def test_get_service_metadata_040(self):
        metadata = self.client.get('/openeo/0.4.0/services/wmts-foo').json
        assert metadata == {
            "id": "wmts-foo",
            "process_graph": {"foo": {"process_id": "foo", "arguments": {}}},
            "url": "https://oeo.net/wmts/foo",
            "type": "WMTS",
            "enabled": True,
            "parameters": {},
            "attributes": {},
            "title": "Test service",
            'submitted': '2020-04-09T15:05:08Z',
        }

    def test_get_service_metadata_100(self):
        metadata = self.client.get('/openeo/1.0.0/services/wmts-foo').json
        assert metadata == {
            "id": "wmts-foo",
            "process": {"process_graph": {"foo": {"process_id": "foo", "arguments": {}}}},
            "url": "https://oeo.net/wmts/foo",
            "type": "WMTS",
            "enabled": True,
            "attributes": {},
            "title": "Test service",
            'created': '2020-04-09T15:05:08Z',
        }

    def test_get_service_metadata_wrong_id(self):
        res = self.client.get('/openeo/1.0.0/services/wmts-invalid')
        assert res.status_code == 404
        assert res.json['code'] == 'ServiceNotFound'


def test_build_backend_deploy_metadata():
    data = build_backend_deploy_metadata(packages=["openeo", "openeo_driver", "foobarblerghbwop"])
    assert data["date"].startswith(datetime.utcnow().strftime("%Y-%m-%dT%H"))
    assert re.match(r"openeo \d+\.\d+\.\d+", data["versions"]["openeo"])
    assert re.match(r"openeo-driver \d+\.\d+\.\d+", data["versions"]["openeo_driver"])
    assert data["versions"]["foobarblerghbwop"] == "n/a"


def test_credentials_basic_no_headers(api):
    api.get("/credentials/basic").assert_error(401, 'AuthenticationRequired')


def test_credentials_basic_wrong_password(api):
    headers = {"Authorization": _build_basic_auth_header(username="john", password="password123")}
    api.get("/credentials/basic", headers=headers).assert_error(403, 'CredentialsInvalid')


def test_credentials_basic(api):
    headers = {"Authorization": _build_basic_auth_header(username="john", password="john123")}
    response = api.get("/credentials/basic", headers=headers).assert_status_code(200).json
    expected = {"access_token"}
    if api.api_version_compare.below("1.0.0"):
        expected.add("user_id")
    assert set(response.keys()) == expected


def test_endpoint_registry():
    app = flask.Flask(__name__)
    bp = flask.Blueprint("test", __name__)
    endpoint = EndpointRegistry()

    @bp.route("/hello")
    def hello():
        return "not an endpoint"

    @endpoint
    @bp.route("/foo")
    def foo():
        return "simple endpoint"

    @endpoint(hidden=True)
    @bp.route("/secret")
    def secret():
        return "secret endpoint"

    @endpoint(version=ComparableVersion("1.0.0").accept_lower)
    @bp.route("/old")
    def old():
        return "old endpoint"

    app.register_blueprint(bp, url_prefix="/bar")

    result = endpoint.get_path_metadata(bp)

    # Check metadata
    assert len(result) == 3
    paths, methods, metadatas = zip(*sorted(result))
    assert paths == ('/foo', '/old', '/secret')
    assert methods == ({"GET"},) * 3
    assert metadatas[0].hidden is False
    assert metadatas[0].for_version is None
    assert metadatas[1].for_version("0.4.0") is True
    assert metadatas[1].for_version("1.0.0") is False
    assert metadatas[2].hidden is True

    # Check that view functions still work
    client = app.test_client()
    assert b"not an endpoint" == client.get("/bar/hello").data
    assert b"simple endpoint" == client.get("/bar/foo").data
    assert b"secret endpoint" == client.get("/bar/secret").data
    assert b"old endpoint" == client.get("/bar/old").data


def test_endpoint_registry_multiple_methods():
    bp = flask.Blueprint("test", __name__)
    endpoint = EndpointRegistry()

    @endpoint
    @bp.route("/foo", methods=["GET"])
    def foo_get():
        return "get"

    @endpoint
    @bp.route("/foo", methods=["POST"])
    def foo_post():
        return "post"

    result = endpoint.get_path_metadata(bp)

    # Check metadata
    assert len(result) == 2
    paths, methods, metadatas = zip(*sorted(result))
    assert paths == ('/foo', '/foo')
    assert methods == ({"GET"}, {"POST"})
