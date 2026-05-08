from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import news, news_analysis, predictions, stocks

app = FastAPI(
    title="Stock Prediction Platform API",
    description="REST API for the Stock Prediction Platform.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(stocks.router)
app.include_router(news.router)
app.include_router(news_analysis.router)
app.include_router(predictions.router)


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}
