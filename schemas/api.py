from typing import Any

from .common import JsonModel, MetricHistoryKey

class MetricHistoryData(MetricHistoryKey):
    values: list[float]

class DeleteModelsResponse(JsonModel):
    errors: dict[str, str]

class TaskData(JsonModel):
    id: str
    full_class_name: str

class JobInputs(JsonModel):
    task_id: str
    args: list[Any]
    kwargs: dict[str, Any]

class AddJobResponse(JsonModel):
    id: str

class StopJobRequest(JsonModel):
    id: str
