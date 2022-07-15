from feast.infra.passthrough_provider import PassthroughProvider


class DkubeProvider(PassthroughProvider):
    """ DkubeProvider class delegates all the operations to
        dkube online and dkube offline store.

    Args:
        PassthroughProvider (type: Provider): passthrough provider
        class.
    """
