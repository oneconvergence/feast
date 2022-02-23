import ipaddress
import sys
import uuid

import base64
from datetime import datetime
from pathlib import Path
from decouple import config as dconfig

from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry_store import RegistryStore
from feast.repo_config import RegistryConfig
from mysql.connector import Error, connect
from requests.structures import CaseInsensitiveDict
from provider.sdk.custom_provider.dkube_client import DkubeClient

from provider.sdk.custom_provider.utils import get_mysql_connect_args


class ProtoRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        # <IP:Port>:user@password:db:registry_name
        reg_config = registry_config.path
        config_args = reg_config.split(":")
        if len(config_args) != 5:
            raise ValueError("")
        self._table = config_args[-1]
        reg_config = ":".join(config_args[:-1])
        self.connect_args = get_mysql_connect_args(reg_config)
        self._initialize_registry()
        DKUBE_IP = dconfig("DKUBE_IP")
        DKUBE_TOKEN = dconfig("DKUBE_TOKEN")
        try:
            ipaddress.ip_address(DKUBE_IP)
        except ValueError:
            sys.exit("Dkube cluster info not properly configured.")
        if DKUBE_TOKEN == "":
            sys.exit("Dkube access token not set.")

        self.dkube = DkubeClient(**{"dkube_ip": DKUBE_IP, "token": DKUBE_TOKEN})

    def _initialize_registry(self):
        return
        try:
            with connect(**self.connect_args) as conn:
                _query = f"""create table if not exists {self._table } (
                         reg_content longblob)"""
                with conn.cursor(buffered=True) as cursor:
                    cursor.execute(_query)
        except Error as err:
            print(err)

    def get_registry_proto(self, **kwargs) -> RegistryProto:
        registry_proto = RegistryProto()
        data = {"project": 'driver_ranking'}
        json_res = self.dkube.get("registry", data=data)
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
        self.dkube.put("registry", data=reg_data)

    def teardown(self, **kwargs):
        reg = {
            'project': kwargs['project']
        }
        self.dkube.delete("registry", data=reg)

    def validate_infra_update_with_registry(self, update_infra):
        json_res = self.dkube.post("registry/validate", data=update_infra)
        if json_res.get('valid') ==  False:
            raise Exception("Invalid registry input.")
