# Minimal FastAPI Backend

This backend is a minimal implementation of the API in `docs/api/design.yaml`.

## Run locally

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

OpenAPI docs:
- Swagger UI: http://localhost:8000/docs
- OpenAPI JSON: http://localhost:8000/openapi.json
