from neptune.alpha.internal.credentials import Credentials
from neptune.exceptions import InvalidNeptuneBackend
from neptune.backend import Backend
from neptune.internal.backends import (
    AlphaIntegrationBackend,
    HostedNeptuneBackend,
    OfflineBackend,
)


def backend_factory(*, backend_name, api_token=None, proxies=None) -> Backend:
    if backend_name == 'offline':
        return OfflineBackend()

    elif backend_name is None:
        credentials = Credentials(api_token)
        # TODO: Improvement. How to determine which backend class should be used?
        if credentials.token_origin_address.startswith('https://alpha.'):
            return AlphaIntegrationBackend(api_token, proxies)

        return HostedNeptuneBackend(api_token, proxies)

    else:
        raise InvalidNeptuneBackend(backend_name)
