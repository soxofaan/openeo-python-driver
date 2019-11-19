import json
import os
from multiprocessing import Pool
from unittest import TestCase, skip

import dummy_impl
from openeo_driver.users import HttpAuthHandler
from openeo_driver.views import app

os.environ["DRIVER_IMPLEMENTATION_PACKAGE"] = "dummy_impl"

client = app.test_client()


class Test(TestCase):

    user_id = 'test'

    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self._auth_header = {
            "Authorization": "Bearer {token}".format(
                token=HttpAuthHandler.encode_basic_access_token(user_id=self.user_id)
            )
        }
        dummy_impl.collections = {}

    def test_health(self):
        resp = self.client.get('/openeo/health')

        assert resp.status_code == 200
        assert "OK" in resp.get_data(as_text=True)

    def test_output_formats(self):
        resp = self.client.get('/openeo/output_formats')
        assert resp.status_code == 200
        assert resp.json == {"GTiff": {"gis_data_types": ["raster"]}, }


    def test_collections(self):
        resp = self.client.get('/openeo/collections')
        assert resp.status_code == 200
        collections = resp.json
        assert 'DUMMY_S2_FAPAR_CLOUDCOVER' in [c['id'] for c in collections['collections']]

    def test_collections_detail(self):
        resp = self.client.get('/openeo/collections/DUMMY_S2_FAPAR_CLOUDCOVER')
        assert resp.status_code == 200
        collection = resp.json
        assert collection['id'] == 'DUMMY_S2_FAPAR_CLOUDCOVER'

    def test_data_detail_error(self):
        resp = self.client.get('/openeo/collections/S2_FAPAR_CLOUDCOVER')
        assert resp.status_code == 404
        error = resp.json
        assert error["code"] == "CollectionNotFound"
        assert error["message"] == "Collection 'S2_FAPAR_CLOUDCOVER' does not exist."

    def test_processes(self):
        resp = self.client.get('/openeo/processes')
        assert resp.status_code == 200
        assert 'max' in {spec['id'] for spec in resp.json['processes']}

    def test_process_details(self):
        resp = self.client.get('/openeo/processes/max')
        assert resp.status_code == 200

    @classmethod
    def _post_download(cls, index):
        download_expected_graph = {'process_id': 'filter_bbox', 'args': {'imagery': {'process_id': 'filter_daterange',
                                                                                     'args': {'imagery': {
                                                                                         'collection_id': 'S2_FAPAR_SCENECLASSIFICATION_V102_PYRAMID'},
                                                                                         'from': '2018-08-06T00:00:00Z',
                                                                                         'to': '2018-08-06T00:00:00Z'}},
                                                                         'left': 5.027, 'right': 5.0438, 'top': 51.2213,
                                                                         'bottom': 51.1974, 'srs': 'EPSG:4326'}}

        post_request = json.dumps({"process_graph": download_expected_graph})
        resp = client.post('/openeo/execute', content_type='application/json', data=post_request)
        assert resp.status_code == 200
        assert resp.content_length > 0
        return index

    @skip
    def test_execute_download_parallel(self):
        """
        Tests downloading in parallel, see EP-2743
        Spark related issues are only exposed/tested when not using the dummy backend
        :return:
        """
        Test._post_download(1)

        with Pool(2) as pool:
            result = pool.map(Test._post_download, range(1, 3))

        print(result)

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


class BatchJobManagementTests(TestCase):
    user_id = 'test'

    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self._auth_header = {
            "Authorization": "Bearer {token}".format(
                token=HttpAuthHandler.encode_basic_access_token(user_id=self.user_id)
            )
        }

    def _create_job(self):
        resp = self.client.post(
            '/openeo/jobs', content_type='application/json', headers=self._auth_header,
            data=json.dumps({'process_graph': {}})
        )
        job_id, user_id = max(dummy_impl.DummyBatchJobManagement.jobs.keys())

        assert user_id == self.user_id
        assert resp.status_code == 201
        assert resp.content_length == 0
        assert resp.headers['Location'].endswith('/openeo/jobs/{i}'.format(i=job_id))
        return job_id

    def _get_actions(self, job_id, user_id=None):
        return dummy_impl.DummyBatchJobManagement.jobs[job_id, user_id or self.user_id]['actions']

    def _get_job_info(self, job_id, user_id=None):
        resp = self.client.get('/openeo/jobs/{i}'.format(i=job_id), headers=self._auth_header)
        assert resp.status_code == 200
        return resp.json

    def test_create_job(self):
        job_id = self._create_job()
        assert self._get_actions(job_id) == ['create']

    def test_start_job(self):
        job_id = self._create_job()
        assert self._get_actions(job_id) == ['create']
        # Start processing it
        resp = self.client.post('/openeo/jobs/{i}/results'.format(i=job_id), headers=self._auth_header)
        assert resp.status_code == 202
        assert self._get_actions(job_id) == ['create', 'start']
        assert self._get_job_info(job_id)['status'] == 'queued'

    def test_start_job_invalid(self):
        resp = self.client.post('/openeo/jobs/foobar/results', headers=self._auth_header)
        assert resp.status_code == 404
        assert resp.json['code'] == 'JobNotFound'

    def test_get_job_info(self):
        job_id = self._create_job()
        resp = self.client.get('/openeo/jobs/{i}'.format(i=job_id), headers=self._auth_header)
        assert resp.status_code == 200
        info = resp.json
        assert info['id'] == job_id
        assert info['status'] == "submitted"

    def test_get_job_info_invalid(self):
        resp = self.client.get('/openeo/jobs/foobar', headers=self._auth_header)
        assert resp.status_code == 404
        assert resp.json['code'] == 'JobNotFound'

    def test_cancel_job(self):
        job_id = self._create_job()
        resp = self.client.delete('/openeo/jobs/{i}/results'.format(i=job_id), headers=self._auth_header)
        assert resp.status_code == 204
        assert self._get_actions(job_id) == ['create', 'cancel']
        assert self._get_job_info(job_id)['status'] == 'canceled'

    def test_api_propagates_http_status_codes(self):
        resp = self.client.get('/openeo/jobs/unknown_job_id/results/some_file', headers=self._auth_header)
        assert resp.status_code == 404
