import asyncio
from contextlib import asynccontextmanager
import logging
from typing import AsyncIterator
from fastapi import APIRouter, FastAPI, HTTPException, Request, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from connection_manager import ConnectionManager
from model_store import MetricHistory, Model, ModelStore
from schemas import DeleteModelsResponse, MetricHistoryRequest, MetricHistoryUpdateData, ModelData, MetricHistoryKey, MetricHistoryData, ModelDeleteData, ModelInsertOrUpdateData, TagRequest

from config import settings

type SubscriptionKey = str | tuple[str, str]

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('pymongo').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

def get_metric_summary(model: Model) -> dict[str, float | None]:
    min_val_loss = None
    max_val_accuracy = None
    history = model.get('training_history')
    if history:
        val_loss: list[float] = history.get('val_loss', [])
        if val_loss:
            min_val_loss = min(val_loss)
        val_accuracy: list[float] = history.get('val_accuracy', [])
        if val_accuracy:
            max_val_accuracy = max(val_accuracy)
    return {'min_val_loss': min_val_loss, 'max_val_accuracy': max_val_accuracy}

def to_model_data(model: Model, metric_summary: dict[str, float | None]) -> ModelData:
    return ModelData(
        datetime=model['datetime'],
        id=model['id'],
        module=model['module'],
        class_name=model['class_name'],
        args=model['args'],
        kwargs=model['kwargs'],
        tag=model['tag'],
        trainable_params=model['trainable_params'],
        status=model['status'],
        **metric_summary)

async def process_model_changes(store: ModelStore, connection_manager: ConnectionManager[SubscriptionKey]) -> None:
    async for change in store.watch_models():
        try:
            match change['type']:
                case 'metric-history.update':
                    message = MetricHistoryUpdateData(**change)
                    data = message.model_dump(by_alias=True)
                    await connection_manager.send_to_subscribers((message.id, message.metric_name), data)
                
                case 'model.insert' | 'model.update':
                    metric_summary = get_metric_summary(change['model'])
                    model_data = to_model_data(change['model'], metric_summary)
                    message = ModelInsertOrUpdateData(type=change['type'], model=model_data)
                    data = message.model_dump(by_alias=True)
                    await connection_manager.send_to_subscribers(message.model.tag, data)

                case 'model.delete':
                    message = ModelDeleteData(type='model.delete', id=change['id'])
                    data = message.model_dump(by_alias=True)
                    await connection_manager.send_to_subscribers(change['tag'], data)
                    
        except Exception:
            logger.exception('Error processing model change')

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.store = ModelStore(settings.model_store.uri, settings.model_store.db)
    app.state.connection_manager = ConnectionManager[SubscriptionKey]()
    model_watcher = asyncio.create_task(process_model_changes(app.state.store, app.state.connection_manager))
    try:
        yield
    finally:
        model_watcher.cancel()
        try:
            await model_watcher
        except asyncio.CancelledError:
            pass
        await app.state.store.close()

def get_model_store(request: Request) -> ModelStore:
    return request.app.state.store

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter(prefix=settings.path_prefix)

@router.get('/tags', response_model=list[str])
async def get_tags(store: ModelStore = Depends(get_model_store)) -> list[str]:
    tags = await store.get_tags()
    return tags

@router.get('/models', response_model=list[ModelData])
async def get_models(tag: str, store: ModelStore = Depends(get_model_store)) -> list[Model]:
    models = []
    async for model in store.get_models(tag):
        metric_summary = get_metric_summary(model)
        models.append(to_model_data(model, metric_summary))
    return models

@router.get('/metric-names', response_model=list[str])
async def get_metric_names(store: ModelStore = Depends(get_model_store)) -> list[str]:
    metrics = sorted([m async for m in store.get_metric_names()])
    return metrics

@router.post('/metric-history', response_model=list[MetricHistoryData])
async def get_metric_history(request: list[MetricHistoryKey], store: ModelStore = Depends(get_model_store)) -> list[MetricHistory]:
    for k in request:
        if not store.is_valid_id(k.id):
            raise HTTPException(status_code=400, detail=f'Invalid id: {k.id}')
    
    requested_keys = [(k.id, k.metric_name) for k in request]
    history_dict = {(h['id'], h['metric_name']): h async for h in store.get_metric_history(requested_keys)}

    histories: list[MetricHistory] = []
    for key in requested_keys:
        history = history_dict.get(key)
        if history is None:
            history = MetricHistory(id=key[0], metric_name=key[1], metric_history=[])
        histories.append(history)

    return histories

@router.websocket('/ws')
async def connect_websocket(socket: WebSocket) -> None:
    logger.debug('Connecting web socket')
    
    connection_manager: ConnectionManager[SubscriptionKey] = socket.app.state.connection_manager
    await connection_manager.connect(socket)

    try:
        while True:
            data = await socket.receive_json()
            message_type = data.get('type')
            match message_type:
                case 'metric-history.subscribe' | 'metric-history.unsubscribe':
                    try:
                        message = MetricHistoryRequest.model_validate(data)
                    except ValidationError:
                        logger.exception('Error validating message: %s', data)
                        continue

                    keys = [(k.id, k.metric_name) for k in message.keys]
                    if message_type == 'metric-history.subscribe':
                        await connection_manager.subscribe(socket, keys)
                    else:
                        await connection_manager.unsubscribe(socket, keys)

                case 'tag.subscribe' | 'tag.unsubscribe':
                    try:
                        message = TagRequest.model_validate(data)
                    except ValidationError:
                        logger.exception('Error validating message: %s', data)
                        continue

                    if message_type == 'tag.subscribe':
                        await connection_manager.subscribe_one(socket, message.tag)
                    else:
                        await connection_manager.unsubscribe_one(socket, message.tag)

                case _:
                    logger.warning('Unknown message type: %s', message_type)

    except WebSocketDisconnect:
        await connection_manager.disconnect_one(socket)
    except Exception:
        logger.exception('Error processing data over web socket')
        await connection_manager.disconnect_one(socket)

@router.post('/delete-models', response_model=DeleteModelsResponse)
async def delete_models(ids: list[str], store: ModelStore = Depends(get_model_store)) -> DeleteModelsResponse:
    exceptions = await store.delete_models(ids)
    return DeleteModelsResponse(errors={id: str(e) for id, e in exceptions.items()})

app.include_router(router)