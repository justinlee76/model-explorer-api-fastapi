# Model Explorer API — FastAPI

FastAPI backend for querying stored model runs, submitting and controlling training jobs, and streaming live model, metric, job, and log updates over WebSockets.

This is one of two interchangeable APIs used by the Model Explorer frontend. Choose this service for Python and native WebSockets; use `model-explorer-api-aspnet` for .NET and SignalR.

## Features

- REST endpoints for models, metrics, training tasks, and jobs.
- Native WebSocket subscriptions for model tags, metric histories, and job logs.
- Async MongoDB access and change-stream processing with PyMongo.
- Pydantic request and response models with camel-case JSON fields.
- Environment-based MongoDB, route-prefix, and CORS configuration.

## Requirements

- Python 3.12+
- A MongoDB replica set or another deployment that supports change streams
- Change-stream pre/post images enabled on the `models` collection

The API and training service must point to the same MongoDB database. The API does not seed training tasks; register at least one task as described in the [training service README](https://github.com/justinlee76/model-explorer-training-service#register-the-built-in-task) before submitting jobs.

## Quick start

Start MongoDB and complete the [MongoDB setup](#mongodb-setup), then run from the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --reload
```

The example configuration starts the API at `http://127.0.0.1:8000` with these documentation endpoints:

- Swagger UI: `http://127.0.0.1:8000/docs`
- ReDoc: `http://127.0.0.1:8000/redoc`
- OpenAPI document: `http://127.0.0.1:8000/openapi.json`

Run Uvicorn from the repository root so the application can import its modules and load `.env`. The `--reload` option is intended for local development.

## Configuration

All application settings are required. Pydantic Settings reads them from `.env` and then applies any process-environment overrides. Double underscores represent nested settings.

| Variable | Example value | Description |
| --- | --- | --- |
| `MODEL_STORE__URI` | `mongodb://localhost:27017` | MongoDB connection URI. |
| `MODEL_STORE__DB` | `model_store` | Database shared with the training service. |
| `PATH_PREFIX` | `/api` | Prefix for every REST and WebSocket route. Start it with `/` and do not end it with `/`; this does not change the documentation routes. |
| `ALLOW_ORIGINS` | `["http://localhost:5173", "http://localhost:5174"]` | JSON array of exact browser origins allowed by CORS. |

The values above are supplied by `.env.example`; they are not defaults in the Python settings model. For example, a shell-based configuration can point at a named local replica set:

```bash
MODEL_STORE__URI='mongodb://localhost:27017/?replicaSet=rs0' \
MODEL_STORE__DB=model_store \
PATH_PREFIX=/api \
ALLOW_ORIGINS='["http://localhost:5173"]' \
uvicorn main:app --reload
```

To connect the Model Explorer frontend to this API, use:

```dotenv
VITE_API_URL=http://localhost:8000/api/
VITE_MESSAGING_TRANSPORT=websocket
```

Keep the trailing slash in `VITE_API_URL`. If Vite selects a different port, add that exact origin to `ALLOW_ORIGINS` and restart the API.

## MongoDB setup

The API watches both `models` and `jobs` with MongoDB change streams. Model deletion notifications also need the deleted document's tag, so the `models` stream requires pre-images.

In `mongosh`, select the configured database, create the collection if necessary, and enable pre/post images:

```javascript
db = db.getSiblingDB("model_store")

if (!db.getCollectionNames().includes("models")) {
  db.createCollection("models")
}

db.runCommand({
  collMod: "models",
  changeStreamPreAndPostImages: { enabled: true }
})
```

Use the value of `MODEL_STORE__DB` if it is not `model_store`. The `jobs` collection also requires a change-stream-capable deployment, but it does not require pre/post images.

## HTTP API

With the example `PATH_PREFIX`, the REST API is available under `/api`. JSON fields use camel case, including `taskId`, `metricName`, `className`, and `modelId`.

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/tags` | List model tags. |
| `GET` | `/api/models?tag=...` | List model runs for a tag. |
| `GET` | `/api/metric-names` | List stored metric names. |
| `POST` | `/api/metric-history` | Return histories for model/metric key objects. |
| `POST` | `/api/delete-models` | Delete model IDs supplied as a JSON string array and return any per-ID errors. |
| `GET` | `/api/tasks` | List registered training tasks. |
| `GET` | `/api/job-defaults` | Return inputs from the latest job, or the first registered task. |
| `POST` | `/api/add-job` | Add a submitted training job. |
| `GET` | `/api/jobs` | List jobs, newest first. |
| `POST` | `/api/stop-job` | Move a job to the stopping state. |
| `DELETE` | `/api/delete-job/{id}` | Delete a job. |
| `GET` | `/api/jobs/{id}/messages` | Return a job's log messages. |

For example, submit a job using a task ID returned by `GET /api/tasks`:

```bash
curl --request POST http://127.0.0.1:8000/api/add-job \
  --header 'Content-Type: application/json' \
  --data '{"taskId":"<task ObjectId>","args":[],"kwargs":{}}'
```

The training service executes submitted jobs and records their status, logs, metrics, and model state. Without a running worker, new jobs remain in the `Submitted` state.

## WebSockets

The WebSocket routes use the same configurable path prefix as the REST API. Client and server messages are JSON, and schema fields use camel case.

| Socket | Client messages | Server messages |
| --- | --- | --- |
| `/api/ws/models` | `tag.subscribe`, `tag.unsubscribe`, `metric-history.subscribe`, `metric-history.unsubscribe` | `model.insert`, `model.update`, `model.delete`, `metric-history.update` |
| `/api/ws/jobs` | `job.messages.subscribe`, `job.messages.unsubscribe` | `job.insert`, `job.update`, `job.delete`, `job.messages.update` |

WebSockets publish changes that happen after the connection is established; they do not send an initial snapshot. Fetch the current models, metric histories, jobs, and messages through the HTTP API before subscribing.

Subscribe to model updates for a tag or to one or more model/metric pairs:

```json
{"type": "tag.subscribe", "tag": "cats_dogs"}
```

```json
{
  "type": "metric-history.subscribe",
  "keys": [
    {"id": "<model ObjectId>", "metricName": "val_loss"}
  ]
}
```

Use the corresponding `*.unsubscribe` type with the same payload to remove a subscription. Model insert, update, and delete events are delivered to subscribers of the model's tag; metric updates are delivered to subscribers of that exact model/metric pair.

Every connection to `/api/ws/jobs` receives job insert, update, and delete events. Subscribe to a job ID to additionally receive its live log messages:

```json
{"type": "job.messages.subscribe", "id": "<job ObjectId>"}
```

The example WebSocket URLs are `ws://127.0.0.1:8000/api/ws/models` and `ws://127.0.0.1:8000/api/ws/jobs`. Use `wss://` when the API is served over HTTPS.

## Storage

| MongoDB data | Purpose |
| --- | --- |
| `models` | Model metadata, status, metric summaries, and metric history. |
| `tasks` | Importable training task module/class registrations. |
| `jobs` | Submitted inputs, status, linked model ID, errors, and logs. |
| GridFS | Serialized model state written by the training service. |

## End-to-end startup order

1. Start a change-stream-capable MongoDB deployment and enable model pre-images.
2. Start this API.
3. Start `model-explorer-training-service` to execute submitted jobs.
4. Start `model-explorer-frontend` with the WebSocket transport.

The worker is not required to browse existing runs, but it is required to process jobs.

## Project layout

| Path | Purpose |
| --- | --- |
| `main.py` | FastAPI application, REST routes, WebSocket handlers, and change-stream tasks. |
| `config.py` | Environment and `.env` settings model. |
| `model_store.py` | Async MongoDB, GridFS, query, mutation, and change-stream operations. |
| `connection_manager.py` | WebSocket connection and subscription management. |
| `model_store_types.py` | Internal enums and typed dictionaries. |
| `schemas/` | Pydantic HTTP and WebSocket schemas. |
| `.env.example` | Local configuration template. |
| `requirements.txt` | Pinned Python dependencies. |

## Deployment and security

Run without `--reload` under a process supervisor or container runtime. If a reverse proxy sits in front of the API, configure it to pass WebSocket upgrades for both WebSocket routes and use HTTPS/WSS outside a trusted local network. Each Uvicorn worker process opens its own two MongoDB change streams and keeps its WebSocket connections and subscriptions in memory.

Authentication and authorization are not configured. The API exposes job submission, stop/delete controls, and model deletion; CORS does not protect these endpoints from non-browser clients. Do not expose the service directly to an untrusted network. Put authentication and HTTPS in front of it for a shared or production deployment.

## Companion projects

| Repository | Role |
| --- | --- |
| [`model-explorer-frontend`](https://github.com/justinlee76/model-explorer-frontend) | React browser interface. |
| [`model-explorer-api-aspnet`](https://github.com/justinlee76/model-explorer-api-aspnet) | Alternative ASP.NET Core and SignalR API. |
| [`model-explorer-training-service`](https://github.com/justinlee76/model-explorer-training-service) | PyTorch worker that executes queued jobs. |
