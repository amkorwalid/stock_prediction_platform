from fastapi import Depends, FastAPI

from app.api import api_router
from app.deps import optional_bearer_auth

app = FastAPI(
    title="Stock Prediction Platform API",
    description="REST API for the Stock Prediction Platform.",
    version="1.0.0",
    dependencies=[Depends(optional_bearer_auth)],
)


@app.get("/health", tags=["system"], include_in_schema=False)
@app.get("/api/health", tags=["system"])
def health() -> dict[str, str]:
    return {"status": "ok"}


from app.routers.news import router as news_router
from app.routers.news_analysis import router as analysis_router
from app.routers.predictions import router as predictions_router
from app.routers.stocks import router as stocks_router

api_router.include_router(stocks_router)
api_router.include_router(news_router)
api_router.include_router(analysis_router)
api_router.include_router(predictions_router)

app.include_router(api_router)
