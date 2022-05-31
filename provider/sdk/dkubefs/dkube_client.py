from functools import wraps
import requests
import json
import urllib3

urllib3.disable_warnings()


class DkubeClient(object):
    def __init__(self, **kwargs) -> None:
        self.dkube_ip = kwargs.get("dkube_ip", "192.168.x.y")
        self.dkube_port = kwargs.get("dkube_port", 32222)
        self.dkube_endpoint = kwargs.get("dkube_endpoint", True)
        self.token = kwargs.get("token", "")
        self.dkube_url = kwargs.get("dkube_url", "")

    def api_endpoint(self, endpoint):
        if not self.dkube_endpoint:
            if self.dkube_url:
                return f"{self.dkube_url}/{endpoint}"
            return f"http://{self.dkube_ip}:{self.dkube_port}/{endpoint}"
        return f"https://{self.dkube_ip}:{self.dkube_port}/dkube/v2/controller/{endpoint}"

    def headers(self, headers=None):
        dkube_headers = {
            "Content-Type": "application/json",
        }
        if self.dkube_endpoint:
            dkube_headers.update(Authorization=f"Bearer {self.token}")
        if headers and isinstance(headers, dict):
            dkube_headers.update(headers)
        return dkube_headers

    def handle_error(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except requests.exceptions.HTTPError as errh:
                raise Exception(f"Call failed. {errh}")
            except requests.exceptions.Timeout as errt:
                raise Exception(f"Call timed out. {errt}")
            except requests.exceptions.ConnectionError as errc:
                raise Exception(f"Connection failed. {errc}")
            except requests.exceptions.RequestException as err:
                raise Exception(f"Exception. {err}")
        return wrapper

    def process_response(self, resp):
        print(resp.request.url)
        if resp.status_code >= 300:
            raise Exception("status code: {} err: {}".format(resp.status_code, resp.text if resp.text else "N/A"))
        parsed_resp = None
        try:
            if resp.headers.get('Content-Type').startswith('application/json'):
                parsed_resp = resp.json()
            else:
                parsed_resp = resp.text
            print(f"status code: {resp.status_code} - {parsed_resp}")
            return parsed_resp
        except Exception as err:
            print(err)
            raise

    @handle_error
    def put(self, endpoint, headers=None, data=None, verify=False, params=None, timeout=45):
        updated_headers = self.headers(headers)
        url = self.api_endpoint(endpoint)
        resp = requests.put(
                url,
                headers=updated_headers,
                data=json.dumps(data),
                verify=verify,
                params=params,
                timeout=timeout)
        return self.process_response(resp)

    @handle_error
    def post(self, endpoint, headers=None, data=None, verify=False, params=None, timeout=45):
        updated_headers = self.headers(headers)
        url = self.api_endpoint(endpoint)
        resp = requests.post(
                url,
                headers=updated_headers,
                data=json.dumps(data),
                verify=verify,
                params=params,
                timeout=timeout)
        return self.process_response(resp)

    @handle_error
    def get(self, endpoint, headers=None, data=None, verify=False, params=None, timeout=45):
        updated_headers = self.headers(headers)
        url = self.api_endpoint(endpoint)
        resp = requests.get(
                url,
                headers=updated_headers,
                data=json.dumps(data),
                verify=verify,
                params=params,
                timeout=timeout)
        return self.process_response(resp)

    @handle_error
    def delete(self, endpoint, headers=None, data=None, verify=False, params=None, timeout=45):
        updated_headers = self.headers(headers)
        url = self.api_endpoint(endpoint)
        resp = requests.delete(
                url,
                headers=updated_headers,
                data=json.dumps(data),
                verify=verify,
                params=params,
                timeout=timeout)
        return self.process_response(resp)
