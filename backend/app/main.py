from fastapi import Depends, FastAPI

from app.deps import optional_bearer_auth
from app.routers.news import router as news_router
from app.routers.news_analysis import router as analysis_router
from app.routers.predictions import router as predictions_router
from app.routers.stocks import router as stocks_router

app = FastAPI(
    title="Stock Prediction Platform API",
    description="REST API for the Stock Prediction Platform.",
    version="1.0.0",
    dependencies=[Depends(optional_bearer_auth)],
)


@app.get("/health", tags=["system"])
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(stocks_router)
app.include_router(news_router)
app.include_router(analysis_router)
app.include_router(predictions_router)
