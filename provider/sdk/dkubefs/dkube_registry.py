import ipaddress
import sys
import uuid

import base64
from datetime import datetime
from pathlib import Path

from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry_store import RegistryStore
from feast.repo_config import RegistryConfig
from provider.sdk.dkubefs.dkube_client import DkubeClient
from provider.sdk.dkubefs.utils import get_registry_config

from dkube.sdk import DkubeApi


class DkubeRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        reg_conf = get_registry_config()
        DKUBE_URL = reg_conf["url"]
        DKUBE_TOKEN = reg_conf["token"]
        if DKUBE_TOKEN == "":
            sys.exit("Dkube access token not set.")
        self.dkube = DkubeApi(URL=DKUBE_URL, token=DKUBE_TOKEN)

    def get_registry_proto(self, **kwargs) -> RegistryProto:
        registry_proto = RegistryProto()
        json_res = self.dkube.get_registry(kwargs['project'])
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
            "regblob": base64.b64encode(registry_proto.SerializeToString()).decode('ascii'),
            "to_add": kwargs['to_add'],
            "to_delete": kwargs['to_delete']
        }
        self.dkube.update_registry(reg_data)

    def teardown(self, **kwargs):
        self.dkube.delete_registry(kwargs['project'])

    def validate_infra_update_with_registry(self, update_infra):
        json_res = self.dkube.validate_project_changes(update_infra)
        if json_res.get('valid') ==  False:
            raise Exception("Invalid registry input.")
