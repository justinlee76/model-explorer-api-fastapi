from datetime import datetime, timezone
import asyncio
import logging
from typing import Any, AsyncIterator

from bson import ObjectId
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError
from gridfs import AsyncGridFSBucket, NoFile

from model_store_types import InvalidIdError, Job, JobArgs, JobStatus, MetricHistory, MetricHistoryUpdate, Model, ModelDelete, ModelInsertOrUpdate, ModelStatus, Task

RETRY_DELAY_SECONDS = 1

logger = logging.getLogger(__name__)

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
        status = ModelStatus(doc['status']),
    )
    if 'training_history' in doc:
        model['training_history'] = doc['training_history']
    return model

def doc_to_task(doc: dict[str, Any]) -> Task:
    return Task(
        id = str(doc['_id']),
        full_class_name=f'{doc["module"]}.{doc["class"]}'
    )

def doc_to_job(doc: dict[str, Any]) -> Job:
    return Job(
        id = str(doc['_id']),
        datetime = doc['datetime'],
        status = JobStatus(doc['status']),
        task_id = str(doc['task_id']),
        args = doc['args'],
        kwargs = doc['kwargs'],
        model_id = str(doc['model_id']) if 'model_id' in doc else None
    )

class ModelStore:
    def __init__(self, uri: str, db: str) -> None:
        self.client = AsyncMongoClient(uri)
        self.db = self.client[db]
        self.bucket = AsyncGridFSBucket(self.db)
    
    async def close(self) -> None:
        await self.client.close()

    def _validate_id(self, id: str):
        if not ObjectId.is_valid(id):
            raise InvalidIdError(id)
    
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
            },
            {
                '$sort': {
                    'metric_name': 1
                }
            }
        ]
        cursor = await self.db.models.aggregate(pipeline)
        async for doc in cursor:
            yield doc['metric_name']
    
    async def get_metric_history(self, keys: list[tuple[str, str]]) -> AsyncIterator[MetricHistory]:
        if not keys:
            return
        
        for i, _ in keys:
            self._validate_id(i)

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

    async def _delete_model(self, id: str) -> None:
        self._validate_id(id)

        obj_id = ObjectId(id)
        status = await self.db.models.find_one({'_id': obj_id}, {'status': 1})
        if status is not None and status.get('status') == ModelStatus.TRAINING.value:
            raise Exception('Model cannot be deleted while training')
        
        result = await self.db.models.delete_one({'_id': obj_id})
        if result.deleted_count == 0:
            return
        
        try:
            await self.bucket.delete(obj_id)
        except NoFile:
            logger.warning('No file found for %s', id)
    
    async def delete_models(self, ids: list[str]) -> dict[str, BaseException]:
        for id in ids:
            self._validate_id(id)

        tasks = [self._delete_model(id) for id in ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        return {id: result for id, result in zip(ids, results) if isinstance(result, BaseException)}
    
    async def get_tasks(self) -> AsyncIterator[Task]:
        async for doc in self.db.tasks.find():
            yield doc_to_task(doc)

    async def get_last_job(self) -> Job | None:
        doc = await self.db.jobs.find_one({}, sort=[('datetime', -1)])
        if doc is None:
            return None
        return doc_to_job(doc)
    
    async def add_job(self, args: JobArgs) -> Job:
        doc = {
            'datetime': datetime.now(timezone.utc),
            'status': JobStatus.SUBMITTED.value,
            'task_id': ObjectId(args['task_id']),
            'args': args['args'],
            'kwargs': args['kwargs']
        }
        result = await self.db.jobs.insert_one(doc)
        doc['_id'] = result.inserted_id
        return doc_to_job(doc)
    
    async def get_jobs(self) -> AsyncIterator[Job]:
        projection = {
            '_id': 1, 
            'datetime': 1, 
            'status': 1, 
            'task_id': 1, 
            'args': 1, 
            'kwargs': 1,
            'model_id': 1
        }
        async for doc in self.db.jobs.find({}, projection, sort=[('datetime', -1)]):
            yield doc_to_job(doc)
    
    async def update_job_status(self, id: str, status: JobStatus) -> bool:
        self._validate_id(id)

        result = await self.db.jobs.update_one({'_id': ObjectId(id)}, {'$set': {'status': status.value}})
        return result.matched_count > 0
    
    async def delete_job(self, id: str) -> bool:
        self._validate_id(id)
        
        result = await self.db.jobs.delete_one({'_id': ObjectId(id)})
        return result.deleted_count > 0