# PyModelStoreApi

FastAPI backend for browsing stored model runs, streaming model/job updates over WebSockets, and submitting training jobs to MongoDB.

## Highlights

- FastAPI + Pydantic DTOs with camelCase JSON aliases
- Async PyMongo access to model, task, and job collections
- WebSocket subscriptions for model tags, metric history, and job logs
- MongoDB change streams for live UI updates
- Environment-driven configuration for public/local clones

## Requirements

- Python 3.12+
- MongoDB replica set or deployment that supports change streams
- The MongoDB `models` collection must have change stream pre/post images enabled:

```javascript
db.runCommand({
  collMod: "models",
  changeStreamPreAndPostImages: { enabled: true }
})
```

## Getting started

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --reload
```

The API defaults to `/api` and allows the Vite dev server on ports `5173` and `5174`.
