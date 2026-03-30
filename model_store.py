import asyncio
import logging
from typing import Any, AsyncIterator

from bson import ObjectId
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

RETRY_DELAY_SECONDS = 1

logger = logging.getLogger(__name__)

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
    
    async def get_models(self, tag: str) -> AsyncIterator[dict[str, Any]]:
        pipeline = [
            {
                '$match': {
                    'tag': tag
                }
            },
            {
                '$project': {
                    'datetime': 1, 
                    '_id': 1, 
                    'module': 1, 
                    'class': 1, 
                    'args': 1, 
                    'kwargs': 1, 
                    'tag': 1, 
                    'trainable_params': 1, 
                    'min_val_loss': {
                        '$min': '$training_history.val_loss'
                    }, 
                    'max_val_accuracy': {
                        '$max': '$training_history.val_accuracy'
                    }, 
                    'status': 1
                }
            }
        ]
        cursor = await self.db.models.aggregate(pipeline)
        async for model in cursor:
            model['id'] = str(model.pop('_id'))
            yield model
    
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
    
    async def get_metric_history(self, keys: list[tuple[str, str]]) -> AsyncIterator[dict[str, Any]]:
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
            doc['id'] = str(doc.pop('_id'))
            yield doc
    
    async def watch_models(self) -> AsyncIterator[dict[str, str|float|int]]:
        pipeline = [
            {
                '$match': {
                    'operationType': 'update'
                }
            }
        ]

        while True:
            try:
                async with await self.db.models.watch(pipeline) as stream:
                    async for change in stream:
                        doc_key = change.get('documentKey') or {}
                        obj_id = doc_key.get('_id')
                        if obj_id is None:
                            continue
                        model_id = str(obj_id)

                        update_description = change.get('updateDescription') or {}
                        updated_fields = update_description.get('updatedFields')
                        if not isinstance(updated_fields, dict):
                            continue

                        for field, value in updated_fields.items():
                            parts = field.split('.')
                            
                            if parts[0] == 'training_history':

                                if len(parts) == 3:
                                    _, metric_name, index_str = parts
                                    if not index_str.isdigit():
                                        continue

                                    index = int(index_str)
                                    yield {
                                        'id': model_id,
                                        'metric_name': metric_name,
                                        'value': value,
                                        'index': index
                                    }

                                elif len(parts) == 1:
                                    if not isinstance(value, dict):
                                        continue

                                    for nested_field, nested_value in value.items():
                                        if not isinstance(nested_value, list):
                                            continue
                                        
                                        for i, v in enumerate(nested_value):
                                            yield {
                                                'id': model_id,
                                                'metric_name': nested_field,
                                                'value': v,
                                                'index': i
                                            }
                                            
            except PyMongoError:
                logger.exception('Exception in change stream')
                await asyncio.sleep(RETRY_DELAY_SECONDS)
