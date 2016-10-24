import logging
logger = logging.getLogger(__name__)
import json
import requests


API_VERSION = 'v32.0'
BULK_JOB_API_VERSION = '36.0'   # not sure why this one doesn't require the leading 'v'


class SFDCException(Exception):
    pass


class SFDCWritesBlocked(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__("You should mock the Salesforce part or set BLOCK_SFDC_WRITES to False in your tests", *args, **kwargs)


class Cursor(object):
    def __init__(self, instance_url, url, payload, headers):
        self._instance_url = instance_url
        self._url = url
        self._payload = payload
        self._headers = headers
        self._next_url = None
        self.results = None
        self._len = None

    def first(self):
        r = requests.get(self._instance_url + self._url, params=self._payload, headers=self._headers)
        if r.status_code >= 400:
            error_message = r.json()[0]['message']
            raise SFDCException(error_message)
        self.results = r.json()
        self._len = self.results['totalSize']
        if 'nextRecordsUrl' in self.results:
            self._next_url = self.results['nextRecordsUrl']

    def next(self):
        if self._next_url is None:
            return False
        r = requests.get(self._instance_url + self._next_url, headers=self._headers)
        if r.status_code >= 400:
            error_message = r.json()[0]['message']
            raise SFDCException(error_message)
        self.results = r.json()
        if 'nextRecordsUrl' in self.results:
            self._next_url = self.results['nextRecordsUrl']
            return True
        else:
            self._next_url = None
            return True

    def __iter__(self):
        return self

    def __len__(self):
        if self.results is None:
            self.first()
        return self._len

    @property
    def iterator(self):
        if self.results is None:
            self.first()
        for record in self.results['records']:
            yield record

        while self.next():
            for record in self.results['records']:
                yield record

        raise StopIteration


# https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_reference_jobinfo.htm
class BulkJob(object):
    OPERATION_DELETE = 'delete'
    OPERATION_INSERT = 'insert'
    OPERATION_QUERY = 'query'
    OPERATION_UPSERT = 'upsert'
    OPERATION_UPDATE = 'update'
    OPERATION_HARD_DELETE = 'hardDelete'

    CONTENT_TYPE_CSV = 'CSV'
    CONTENT_TYPE_JSON = 'JSON'
    CONTENT_TYPE_XML = 'XML'
    CONTENT_TYPE_ZIP_CSV = 'ZIP_CSV'
    CONTENT_TYPE_ZIP_JSON = 'ZIP_JSON'
    CONTENT_TYPE_ZIP_XML = 'ZIP_XML'

    def __init__(self, operation, object_for_job, content_type, instance_url, access_token):
        self._instance_url = instance_url
        self._access_token = access_token
        self._object = object_for_job
        self._operation = operation
        self._content_type = content_type
        self._response = None

    def create(self):
        payload = {
            "operation": self._operation,
            "object": self._object,
            "contentType": self._content_type,
        }
        r = self._dispatch_command(payload)
        self._response = r

    def _dispatch_command(self, payload):
        headers = {
            'X-SFDC-Session': self._access_token,
            'Content-Type': 'application/json',
        }
        if self._response is None:
            url = '{instance}/services/async/{version}/job'.format(version=BULK_JOB_API_VERSION,
                                                                   instance=self._instance_url)
        else:
            url = '{instance}/services/async/{version}/job/{jobId}'.format(version=BULK_JOB_API_VERSION,
                                                                           instance=self._instance_url,
                                                                           jobId=self._response['id'])

        # raise Exception('err')
        r = requests.post(url, data=json.dumps(payload), headers=headers)
        if r.status_code >= 400:
            code = r.json()['exceptionCode']
            message = r.json()['exceptionMessage']
            raise SFDCException(code + ' : ' + message)
        return r.json()

    # add a batch of data to this job, the content should be of the type specified in __init__
    # https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_batches_create.htm
    def add_batch(self, content):
        headers = {
            'X-SFDC-Session': self._access_token,
            'Content-Type': 'application/json',
        }

        url = '{instance}/services/async/{version}/job/{jobId}/batch'.format(version=BULK_JOB_API_VERSION,
                                                                             instance=self._instance_url,
                                                                             jobId=self._response['id'])

        r = requests.post(url, data=content, headers=headers)
        if r.status_code >= 400:
            raise Exception(r.text)
        return r.json()

    # https: // developer.salesforce.com / docs / atlas.en - us.api_asynch.meta / api_asynch / asynch_api_jobs_abort.htm
    def abort(self):
        r = self._dispatch_command({"state": "Aborted"})
        return r

    # close the job
    def close(self):
        r = self._dispatch_command({"state": "Closed"})
        return r


class SFDC(object):

    def __init__(self, login_endpoint=None, client_id=None, client_secret=None, username=None, password=None, token=None, block_sfdc_writes=False):

        self._login_endpoint = login_endpoint
        self._client_id = client_id
        self._client_secret = client_secret
        self._username = username
        self._password = password
        self._token = token
        self._block_sfdc_writes = block_sfdc_writes

        self._instance_url = None
        self._access_token = None

    def login(self):
        payload = {
            'grant_type': 'password',
            'format': 'json',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'username': self._username,
            'password': self._password + self._token
        }
        headers = {
            'Accept': 'application/json'
        }

        r = requests.post(self._login_endpoint, data=payload, headers=headers)
        json_response = r.json()
        if r.status_code >= 400:
            err = SFDCException(json_response['error_description'])
            raise err

        self._instance_url = json_response['instance_url']
        self._access_token = json_response['access_token']

    def _authenticate(self):
        if not self._access_token:
            self.login()

        headers = {
            'Authorization': 'Bearer ' + self._access_token,
        }
        return headers

    def _response(self, r):
        if r.status_code >= 400:
            error_message = r.json()[0]['message']
            raise SFDCException(error_message)

        return r.json()

    def get_cursor(self, url, payload):
        headers = self._authenticate()
        cursor = Cursor(instance_url=self._instance_url, url=url, payload=payload, headers=headers)
        cursor.first()
        return cursor

    def get(self, url, payload={}):
        headers = self._authenticate()
        r = requests.get(self._instance_url + url, params=payload, headers=headers)
        return self._response(r)

    def post(self, url, payload):
        if self._block_sfdc_writes:
            raise SFDCWritesBlocked()

        headers = self._authenticate()
        headers['Content-Type'] = 'application/json'
        r = requests.post(self._instance_url + url, data=json.dumps(payload), headers=headers)
        return self._response(r)

    def patch(self, url, payload):
        if self._block_sfdc_writes:
            raise SFDCWritesBlocked()

        headers = self._authenticate()
        headers['Content-Type'] = 'application/json'
        r = requests.patch(self._instance_url + url, data=json.dumps(payload), headers=headers)
        if r.status_code >= 400:
            error_message = r.json()[0]['message']
            raise SFDCException(error_message)
        if r.status_code != 204:    # 204 == No content, means existing object updated
            return r.json()

    def put(self, url, payload):
        if self._block_sfdc_writes:
            raise SFDCWritesBlocked()

        headers = self._authenticate()
        headers['Content-Type'] = 'application/json'
        r = requests.put(self._instance_url + url, data=json.dumps(payload), headers=headers)
        return self._response(r)

    def delete(self, url):
        if self._block_sfdc_writes:
            raise SFDCWritesBlocked()

        headers = self._authenticate()
        headers['Content-Type'] = 'application/json'
        r = requests.delete(self._instance_url + url, headers=headers)
        if r.status_code >= 400:
            error_message = r.json()[0]['message']
            raise SFDCException(error_message)
        return {
            'status_code': r.status_code
        }

    def query_cursor(self, query):
        url = '/services/data/{}/query'.format(API_VERSION)
        r = self.get_cursor(url=url, payload=dict(q=query))
        return r

    def query(self, query):
        url = '/services/data/{}/query'.format(API_VERSION)
        r = self.get(url=url, payload=dict(q=query))

        return r

    def query_all(self, query):
        cursor = self.query_cursor(query)
        result = cursor.results['records']
        while cursor.next():
            result += cursor.results['records']

        return result

    def create_job(self, operation, object, content_type):
        safe_operations = [BulkJob.OPERATION_QUERY,]
        if self._block_sfdc_writes and operation not in safe_operations:
            raise SFDCWritesBlocked()

        job = BulkJob(operation, object, content_type, instance_url=self._instance_url, access_token=self._access_token)
        job.create()
        return job

