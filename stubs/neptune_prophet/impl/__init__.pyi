from typing import (
    Any,
    Dict,
    Mapping,
    Optional,
)

import pandas as pd

from neptune.types import File

Prophet = Any

def create_summary(
    model: Prophet,
    df: Optional[pd.DataFrame] = ...,
    fcst: Optional[pd.DataFrame] = ...,
    log_charts: bool = ...,
    log_interactive: bool = ...,
) -> Mapping: ...
def get_model_config(model: Prophet) -> Dict[str, Any]: ...
def get_serialized_model(model: Prophet) -> File: ...
def get_forecast_components(model: Prophet, fcst: pd.DataFrame) -> Dict[str, Any]: ...
def create_forecast_plots(model: Prophet, fcst: pd.DataFrame, log_interactive: bool = ...) -> Dict[str, Any]: ...
def create_residual_diagnostics_plots(
    fcst: pd.DataFrame, y: pd.Series, log_interactive: bool = ..., alpha: float = ...
) -> Dict[str, Any]: ...
