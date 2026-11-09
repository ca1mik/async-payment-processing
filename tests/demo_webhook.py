from collections import defaultdict
from time import monotonic

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

app = FastAPI()
received = defaultdict(list)


@app.post("/hook/{scenario}")
async def hook(scenario: str, request: Request, idempotency_key: str = Header()):
    calls = received[scenario]
    calls.append({"key": idempotency_key, "data": await request.json(), "time": monotonic()})
    if scenario == "fail" or (scenario == "retry" and len(calls) < 3):
        return JSONResponse({"received": False}, status_code=503)
    return {"received": True}


@app.get("/events/{scenario}")
async def events(scenario: str):
    return received[scenario]
