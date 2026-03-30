import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Any, AsyncIterator
from fastapi import APIRouter, FastAPI, HTTPException, Request, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from connection_manager import ConnectionManager
from model_store import ModelStore
from schemas import MetricHistoryRequest, MetricHistoryUpdate, ModelData, MetricHistoryKey, MetricHistoryData

from config import settings

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('pymongo').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

async def process_model_changes(store: ModelStore, connection_manager: ConnectionManager[tuple[str, str]]) -> None:
    async for change in store.watch_models():
        try:
            id = str(change['id'])
            metric_name = str(change['metric_name'])
            value = float(change['value'])
            index = int(change['index'])
            message = MetricHistoryUpdate(type='metric-history.update', id=id, metric_name=metric_name, value=value, index=index)
            data = message.model_dump(by_alias=True)
            await connection_manager.send_to_subscribers((id, metric_name), data)
        except Exception:
            logger.exception('Error processing model change')

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.store = ModelStore(settings.model_store.uri, settings.model_store.db)
    app.state.connection_manager = ConnectionManager[tuple[str, str]]()
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
async def get_models(tag: str, store: ModelStore = Depends(get_model_store)) -> list[dict[str, Any]]:
    models = [m async for m in store.get_models(tag)]
    return models

@router.get('/metric-names', response_model=list[str])
async def get_metric_names(store: ModelStore = Depends(get_model_store)) -> list[str]:
    metrics = sorted([m async for m in store.get_metric_names()])
    return metrics

@router.post('/metric-history', response_model=list[MetricHistoryData])
async def get_metric_history(request: list[MetricHistoryKey], store: ModelStore = Depends(get_model_store)) -> list[dict[str, Any]]:
    for k in request:
        if not store.is_valid_id(k.id):
            raise HTTPException(status_code=400, detail=f'Invalid id: {k.id}')
    
    requested_keys = [(k.id, k.metric_name) for k in request]
    history_dict = {(h['id'], h['metric_name']): h async for h in store.get_metric_history(requested_keys)}

    histories = []
    for key in requested_keys:
        history = history_dict.get(key)
        if history is None:
            history = {'id': key[0], 'metric_name': key[1], 'metric_history': []}
        histories.append(history)

    return histories

@router.websocket('/ws')
async def connect_websocket(socket: WebSocket) -> None:
    logger.debug('Connecting web socket')
    
    connection_manager: ConnectionManager[tuple[str, str]] = socket.app.state.connection_manager
    await connection_manager.connect(socket)

    try:
        while True:
            data = await socket.receive_json()
            message = None
            message_type = data.get('type')
            if message_type == 'metric-history.subscribe' or message_type == 'metric-history.unsubscribe':
                try:
                    message = MetricHistoryRequest.model_validate(data) 
                except ValidationError:
                    logger.exception('Error validating message')
            if message is None:
                continue

            key = message.id, message.metric_name
            if message_type == 'metric-history.subscribe':
                await connection_manager.subscribe(socket, key)
            elif message_type == 'metric-history.unsubscribe':
                await connection_manager.unsubscribe(socket, key)

    except WebSocketDisconnect:
        await connection_manager.disconnect(socket)
    except Exception:
        logger.exception('Error processing data over web socket')
        await connection_manager.disconnect(socket)

app.include_router(router)