import base64
import uuid
from datetime import datetime
from pathlib import Path

from dkube.sdk import DkubeApi
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry_store import RegistryStore
from feast.repo_config import RegistryConfig
from online_server.common.utils.utils import get_user_info_by_project
from provider.sdk.dkubefs.utils import (get_dkube_client, get_registry_config,
                                        get_user_token)


class DkubeRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        reg_conf = get_registry_config()
        self.dkube_url = reg_conf["url"]

    def get_dkube_client(self, project):
        user_info = get_user_info_by_project(project)
        if "token" not in user_info:
            raise Exception("Token not found.")
        return DkubeApi(URL=self.dkube_url, token=user_info["token"])

    def get_registry_proto(self, **kwargs) -> RegistryProto:
        registry_proto = RegistryProto()
        project = kwargs['project']
        dkube_client = self.get_dkube_client(project)
        json_res = dkube_client.get_registry(project)
        if 'regblob' in json_res and json_res['regblob']:
            reg_proto = base64.b64decode(json_res['regblob'].encode('ascii'))
            registry_proto.ParseFromString(reg_proto)
            return registry_proto
        else:
            print("No details found in project registry.")
            return RegistryProto()

    def update_registry_proto(self, registry_proto: RegistryProto, **kwargs):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        project = kwargs['project']
        reg_data = {
            "project": project,
            "regblob": base64.b64encode(
                registry_proto.SerializeToString()).decode('ascii'),
            "to_add": kwargs['to_add'],
            "to_delete": kwargs['to_delete']
        }
        dkube_client = get_dkube_client(project)
        dkube_client.update_registry(reg_data)

    def teardown(self, **kwargs):
        dkube_client = self.get_dkube_client()
        dkube_client.delete_registry(kwargs['project'])

    def validate_infra_update_with_registry(self, update_infra):
        json_res = self.dkube.validate_project_changes(update_infra)
        if json_res.get('valid') is False:
            raise Exception("Invalid registry input.")
