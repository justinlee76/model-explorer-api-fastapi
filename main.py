from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from fastapi import APIRouter, FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware

from model_store import ModelStore
from schemas import ModelData, MetricHistoryKey, MetricHistoryData

from config import settings

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.store = ModelStore(settings.model_store.uri, settings.model_store.db)
    yield
    await app.state.store.close()

def get_model_store(request: Request) -> ModelStore:
    return request.app.state.store

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.allow_origin],
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
    metrics = [m async for m in store.get_metric_names()]
    return metrics

@router.post('/metric-history', response_model=list[MetricHistoryData])
async def get_metric_history(request: list[MetricHistoryKey], store: ModelStore = Depends(get_model_store)) -> list[dict[str, Any]]:
    for k in request:
        if not store.is_valid_id(k.id):
            raise HTTPException(status_code=400, detail=f'Invalid id: {k.id}')
    history = [h async for h in store.get_metric_history([(k.id, k.metric_name) for k in request])]
    return history

app.include_router(router)