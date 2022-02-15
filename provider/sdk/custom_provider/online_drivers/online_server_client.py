from provider.sdk.custom_provider.dkube_client import DkubeClient


class OnlineServerClient(DkubeClient):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
