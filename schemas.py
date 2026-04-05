from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from pydantic.alias_generators import to_camel

class ModelData(BaseModel):
    model_config = ConfigDict(
        alias_generator = to_camel,
        validate_by_name=True,
        validate_by_alias=True
    )
    datetime: datetime
    id: str
    module: str
    class_name: str
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

class TagRequest(JsonModel):
    type: Literal['tag.subscribe', 'tag.unsubscribe']
    tag: str

class MetricHistoryKey(JsonModel):
    id: str
    metric_name: str
    
class MetricHistoryData(MetricHistoryKey):
    metric_history: list[float]

class MetricHistoryRequest(JsonModel):
    type: Literal['metric-history.subscribe', 'metric-history.unsubscribe']
    keys: list[MetricHistoryKey]

class MetricHistoryUpdateData(JsonModel):
    type: Literal['metric-history.update']
    id: str
    metric_name: str
    index: int
    value: float

class ModelInsertOrUpdateData(JsonModel):
    type: Literal['model.insert', 'model.update']
    model: ModelData

class ModelDeleteData(JsonModel):
    type: Literal['model.delete']
    id: str
