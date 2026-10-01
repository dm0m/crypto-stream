from fastapi import FastAPI

from api.routers import candles, health, stats

app = FastAPI()
app.include_router(candles.router)
app.include_router(stats.router)
app.include_router(health.router)
