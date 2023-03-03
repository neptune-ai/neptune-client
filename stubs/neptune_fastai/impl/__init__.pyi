from typing import (
    Any,
    Optional,
    Union,
)

from neptune import Run
from neptune.handler import Handler

class NeptuneCallback:
    def __init__(
        self,
        run: Union[Run, Handler],
        base_namespace: str = ...,
        upload_saved_models: Optional[str] = ...,
        **kwargs: Any,
    ) -> None: ...
