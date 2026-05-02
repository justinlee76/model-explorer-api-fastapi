from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

class JsonModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        validate_by_name=True,
        validate_by_alias=True
    )

class ModelData(JsonModel):
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
    status: str

class MetricHistoryKey(JsonModel):
    id: str
    metric_name: str
    
class JobData(JsonModel):
    id: str
    datetime: datetime
    task_id: str
    args: list[Any]
    kwargs: dict[str, Any]
    status: str
    model_id: str | None