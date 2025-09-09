from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import redis
import json

app = FastAPI(title="Fraud Detection Dashboard", version="1.0")

# Connect to Redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Templates
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/transactions")
def get_transactions():
    transactions = []
    while True:
        tx_data = redis_client.rpop("transactions")
        if tx_data is None:
            break
        transactions.append(json.loads(tx_data))
    return JSONResponse(content=transactions)

@app.get("/health")
def health():
    return {"status": "healthy"}
