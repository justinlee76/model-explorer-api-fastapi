from enum import Enum
from typing import Any, Literal, TypedDict
from datetime import datetime

class InvalidIdError(ValueError):
    def __init__(self, id: str):
        super().__init__(f'Invalid id: {id}')

class ModelStatus(Enum):
    TRAINING = 0
    TRAINED = 1

class ModelRequired(TypedDict):
    datetime: datetime
    id: str
    module: str
    class_name: str
    args: list[Any]
    kwargs: dict[str, Any]
    tag: str
    trainable_params: int
    status: ModelStatus

class Model(ModelRequired, total=False):
    training_history: dict[str, list[float]]

class MetricHistory(TypedDict):
    id: str
    metric_name: str
    metric_history: list[float]

class MetricHistoryUpdate(TypedDict):
    type: Literal['metric-history.update']
    id: str
    metric_name: str
    index: int
    value: float

class ModelInsertOrUpdate(TypedDict):
    type: Literal['model.insert', 'model.update']
    model: Model

class ModelDelete(TypedDict):
    type: Literal['model.delete']
    tag: str
    id: str

class Task(TypedDict):
    id: str
    full_class_name: str

class JobStatus(Enum):
    SUBMITTED = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3
    STOPPING = 4
    STOPPED = 5

class JobArgs(TypedDict):
    task_id: str
    args: list[Any]
    kwargs: dict[str, Any]    

class Job(JobArgs):
    id: str
    datetime: datetime
    status: JobStatus
    model_id: str | None

