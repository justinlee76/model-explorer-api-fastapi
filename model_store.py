from typing import Any, AsyncIterator

from bson import ObjectId
from pymongo import AsyncMongoClient

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
