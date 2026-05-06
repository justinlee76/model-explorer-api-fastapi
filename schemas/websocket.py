from typing import Literal

from .common import JsonModel, MetricHistoryKey, ModelData, JobData

class TagRequest(JsonModel):
    type: Literal['tag.subscribe', 'tag.unsubscribe']
    tag: str

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

class JobInsertOrUpdateData(JsonModel):
    type: Literal['job.insert', 'job.update']
    job: JobData

class JobDeleteData(JsonModel):
    type: Literal['job.delete']
    id: str

class JobMessagesRequest(JsonModel):
    type: Literal['job.messages.subscribe', 'job.messages.unsubscribe']
    id: str

class JobMessagesUpdateData(JsonModel):
    type: Literal['job.messages.update']
    id: str
    index: int
    message: str