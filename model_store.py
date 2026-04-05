from datetime import datetime
import asyncio
import logging
from typing import Any, AsyncIterator, Literal, TypedDict

from bson import ObjectId
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

RETRY_DELAY_SECONDS = 1

logger = logging.getLogger(__name__)

class ModelRequired(TypedDict):
    datetime: datetime
    id: str
    module: str
    class_name: str
    args: list[Any]
    kwargs: dict[str, Any]
    tag: str
    trainable_params: int
    status: int

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

def doc_to_model(doc: dict[str, Any]) -> Model:
    model = Model(
        datetime = doc['datetime'],
        id = str(doc['_id']),
        module = doc['module'],
        class_name = doc['class'],
        args = doc['args'],
        kwargs = doc['kwargs'],
        tag = doc['tag'],
        trainable_params = doc['trainable_params'],
        status = doc['status'],
    )
    if 'training_history' in doc:
        model['training_history'] = doc['training_history']
    return model

class ModelStore:
    def __init__(self, uri: str, db: str) -> None:
        self.client = AsyncMongoClient(uri)
        self.db = self.client[db]
    
    async def close(self) -> None:
        await self.client.close()

    def is_valid_id(self, id: str) -> bool:
        return ObjectId.is_valid(id)
    
    async def get_tags(self) -> list[str]:
        tags = await self.db.models.distinct('tag')
        return tags
    
    async def get_models(self, tag: str) -> AsyncIterator[Model]:
        cursor = self.db.models.find(
            {'tag': tag},
            {
                'datetime': 1, 
                '_id': 1, 
                'module': 1, 
                'class': 1, 
                'args': 1, 
                'kwargs': 1, 
                'tag': 1, 
                'trainable_params': 1, 
                'status': 1,
                'training_history': 1
            })
        async for doc in cursor:
            yield doc_to_model(doc)
    
    async def get_metric_names(self) -> AsyncIterator[str]:
        pipeline = [
            {
                '$project': {
                    '_id': 0, 
                    'metrics': {
                        '$objectToArray': '$training_history'
                    }
                }
            }, 
            {
                '$unwind': {
                    'path': '$metrics'
                }
            }, 
            {
                '$group': {
                    '_id': '$metrics.k'
                }
            }, 
            {
                '$project': {
                    'metric_name': '$_id', 
                    '_id': 0
                }
            }
        ]
        cursor = await self.db.models.aggregate(pipeline)
        async for doc in cursor:
            yield doc['metric_name']
    
    async def get_metric_history(self, keys: list[tuple[str, str]]) -> AsyncIterator[MetricHistory]:
        pipeline = [
            {
                '$project': {
                    '_id': 1, 
                    'metrics': {
                        '$objectToArray': '$training_history'
                    }
                }
            },
            {
                '$unwind': {
                    'path': '$metrics', 
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$match': {
                    '$or': [
                        { '_id': ObjectId(i), 'metrics.k': m } for i, m in keys
                    ]
                }
            },
            {
                '$project': {
                    '_id': 1, 
                    'metric_name': '$metrics.k', 
                    'metric_history': '$metrics.v'
                }
            }
        ]
        cursor = await self.db.models.aggregate(pipeline)
        async for doc in cursor:
            history = doc.copy()
            history['id'] = str(history.pop('_id'))
            yield history
    
    async def watch_models(self) -> AsyncIterator[MetricHistoryUpdate | ModelInsertOrUpdate | ModelDelete]:
        pipeline = [
            {
                '$match': {
                    'operationType': {'$in': ['insert', 'update', 'delete']}
                }
            }
        ]

        while True:
            try:
                async with await self.db.models.watch(pipeline, full_document='updateLookup', full_document_before_change='required') as stream:
                    async for change in stream:
                        doc_key = change.get('documentKey') or {}
                        obj_id = doc_key.get('_id')
                        if obj_id is None:
                            continue

                        model_id = str(obj_id)
                        if change['operationType'] == 'delete':
                            full_doc = change.get('fullDocumentBeforeChange')
                            if full_doc is None:
                                logger.warning('fullDocumentBeforeChange not found for delete operation - has changeStreamPreAndPostImages been enabled?')
                                continue
                        else:
                            full_doc = change.get('fullDocument')

                        model = doc_to_model(full_doc)

                        match change['operationType']:
                            case 'insert':
                                yield {
                                    'type': 'model.insert',
                                    'model': model
                                }

                            case 'update':
                                update_description = change.get('updateDescription') or {}
                                updated_fields = update_description.get('updatedFields')
                                if not isinstance(updated_fields, dict):
                                    continue

                                send_model_update = False
                                for field, value in updated_fields.items():
                                    parts = field.split('.')
                                    
                                    if parts[0] == 'training_history':
                                        send_model_update = True

                                        if len(parts) == 3:
                                            _, metric_name, index_str = parts
                                            if not index_str.isdigit():
                                                continue

                                            index = int(index_str)
                                            yield {
                                                'type': 'metric-history.update',
                                                'id': model_id,
                                                'metric_name': metric_name,
                                                'index': index,
                                                'value': value
                                            }

                                        elif len(parts) == 1:
                                            if not isinstance(value, dict):
                                                continue

                                            for nested_field, nested_value in value.items():
                                                if not isinstance(nested_value, list):
                                                    continue
                                                
                                                for i, v in enumerate(nested_value):
                                                    yield {
                                                        'type': 'metric-history.update',
                                                        'id': model_id,
                                                        'metric_name': nested_field,
                                                        'index': i,
                                                        'value': v
                                                    }

                                    elif field == 'status':
                                        send_model_update = True
                                
                                if send_model_update:
                                    yield {
                                        'type': 'model.update',
                                        'model': model
                                    }

                            case 'delete':
                                yield {
                                    'type': 'model.delete',
                                    'tag': model.get('tag'),
                                    'id': model_id
                                }
                                            
            except PyMongoError:
                logger.exception('Exception in change stream')
                await asyncio.sleep(RETRY_DELAY_SECONDS)
