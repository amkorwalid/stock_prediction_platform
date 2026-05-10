# Minimal FastAPI Backend

This backend is a minimal implementation of the API in `docs/api/design.yaml`.

## Configuration

Set either:

- `BACKEND_DATABASE_URL`, or
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `BACKEND_USER_USERNAME`, `BACKEND_USER_PASSWORD`

## Run locally

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

OpenAPI docs:

- Swagger UI: http://localhost:8000/docs
- OpenAPI JSON: http://localhost:8000/openapi.json

Base API path:

- Local FastAPI routes are served under http://localhost:8000/api
- This matches an Nginx reverse proxy that forwards /api/\* to the backend
- Health check is available at http://localhost:8000/api/health
