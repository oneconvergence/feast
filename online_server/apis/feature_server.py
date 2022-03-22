import fastapi
from fastapi import HTTPException, Request
from fastapi.logger import logger
from google.protobuf.json_format import MessageToDict, Parse

import feast
from feast import proto_json
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest
from feast.type_map import feast_value_type_to_python_type

from common.utils.utils import get_repo_path


router = fastapi.APIRouter()


@router.get("/get-online-features")
async def get_online_features(request: Request):
    proto_json.patch()
    try:
        # Validate and parse the request data into GetOnlineFeaturesRequest Protobuf object
        body = await request.body()
        request_proto = GetOnlineFeaturesRequest()
        Parse(body, request_proto)

        repo_path = get_repo_path()
        store = feast.FeatureStore(repo_path=repo_path)

        # Initialize parameters for FeatureStore.get_online_features(...) call
        if request_proto.HasField("feature_service"):
            features = store.get_feature_service(request_proto.feature_service)
        else:
            features = list(request_proto.features.val)

        full_feature_names = request_proto.full_feature_names

        batch_sizes = [len(v.val) for v in request_proto.entities.values()]
        num_entities = batch_sizes[0]
        if any(batch_size != num_entities for batch_size in batch_sizes):
            raise HTTPException(status_code=500, detail="Uneven number of columns")

        entity_rows = [
            {
                k: feast_value_type_to_python_type(v.val[idx])
                for k, v in request_proto.entities.items()
            }
            for idx in range(num_entities)
        ]

        response_proto = store.get_online_features(
            features, entity_rows, full_feature_names=full_feature_names
        ).proto

        # Convert the Protobuf object to JSON and return it
        return MessageToDict(  # type: ignore
            response_proto, preserving_proto_field_name=True, float_precision=18
        )
    except Exception as e:
        # Print the original exception on the server side
        logger.exception(e)
        # Raise HTTPException to return the error message to the client
        raise HTTPException(status_code=500, detail=str(e))
