from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from services import fetch_locations, fetch_weather
from contextlib import asynccontextmanager
from mangum import Mangum
import httpx
import redis.asyncio as redis
import os


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient()

    REDIS_URL = os.getenv("REDIS_URL")
    app.state.redis = redis.from_url(REDIS_URL, decode_responses=True)

    yield

    await app.state.client.aclose()
    await app.state.redis.close()

app = FastAPI(lifespan=lifespan)
handler = Mangum(app, lifespan="on")

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

@app.get('/')
def home():
    return {'status': 'Weather Backend is running!'}


@app.get('/locations')
async def get_locations(name: str, request: Request):
    results = await fetch_locations(name, request.app.state.client)

    if results is None:
        raise HTTPException(status_code=503, detail='Search function currently unavailable')
    
    return results

@app.get('/weather')
async def get_weather(lat: float, lon: float, request: Request):
    try:
        client = request.app.state.client
        redis_client = request.app.state.redis

        results = await fetch_weather(lat, lon, client, redis_client)
        return results
    except Exception as e:
        raise HTTPException(status_code=503, detail='Weather service unavailable')

