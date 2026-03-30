from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from pydantic.alias_generators import to_camel

def model_alias_generator(name: str) -> str:
    match name:
        case 'class_':
            return 'class'
        case 'kwargs':
            return 'kwArgs'
        case _:
            return to_camel(name)

class ModelData(BaseModel):
    model_config = ConfigDict(
        alias_generator = model_alias_generator,
        validate_by_name=True,
        validate_by_alias=True
    )
    datetime: datetime
    id: str
    module: str
    class_: str
    args: list[Any]
    kwargs: dict[str, Any]
    tag: str
    trainable_params: int
    min_val_loss: float | None
    max_val_accuracy: float | None
    status: int

class JsonModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        validate_by_name=True,
        validate_by_alias=True
    )

class MetricHistoryKey(JsonModel):
    id: str
    metric_name: str
    
class MetricHistoryData(JsonModel):
    id: str
    metric_name: str
    metric_history: list[float]

class MetricHistoryRequest(JsonModel):
    type: Literal['metric-history.subscribe', 'metric-history.unsubscribe']
    id: str
    metric_name: str

class MetricHistoryUpdate(JsonModel):
    type: Literal['metric-history.update']
    id: str
    metric_name: str
    value: float
    index: int

