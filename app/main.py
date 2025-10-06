from fastapi import FastAPI, HTTPException, Request, Header, Depends
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import json
import time

from app.schemas import Book, RatingIn, RatingOut, ErrorResponse


app = FastAPI(
    title="GoodBooks API",
    description="A simple FastAPI + MongoDB project for the GoodBooks-10k dataset",
    version="1.0.0"
)

# -----------------------
# Database Setup
# -----------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["goodbooks"]

# -----------------------
# Utility: JSONL Logging
# -----------------------
def log_request(route: str, params: dict, status: int, latency_ms: float, client_ip: str):
    log_entry = {
        "route": route,
        "params": params,
        "status": status,
        "latency_ms": round(latency_ms, 2),
        "client_ip": client_ip,
        "ts": datetime.utcnow().isoformat()
    }
    with open("access.log.jsonl", "a") as f:
        f.write(json.dumps(log_entry) + "\n")


# -----------------------
# Auth Dependency
# -----------------------
API_KEY = "secret123"  # you can change this
def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# -----------------------
# Routes
# -----------------------
@app.get("/", response_model=dict)
def home():
    return {"message": "Welcome to the Goodbooks API"}


@app.get("/books/{book_id}", response_model=Book, responses={404: {"model": ErrorResponse}})
def get_book(request: Request, book_id: int):
    start = time.time()
    book = db.books.find_one({"book_id": book_id})
    if not book:
        log_request(str(request.url), {"book_id": book_id}, 404, (time.time() - start)*1000, request.client.host)
        raise HTTPException(status_code=404, detail="Book not found")

    book["_id"] = str(book["_id"])
    log_request(str(request.url), {"book_id": book_id}, 200, (time.time() - start)*1000, request.client.host)
    return book


@app.get("/search", response_model=list[Book])
def search_books(request: Request, q: str, limit: int = 10):
    start = time.time()
    books = list(db.books.find({"title": {"$regex": q, "$options": "i"}}).limit(limit))
    for b in books:
        b["_id"] = str(b["_id"])
    log_request(str(request.url), {"q": q, "limit": limit}, 200, (time.time() - start)*1000, request.client.host)
    return books


# -----------------------
# POST /ratings
# -----------------------
@app.post("/ratings", response_model=RatingOut, responses={401: {"model": ErrorResponse}, 400: {"model": ErrorResponse}})
def create_rating(request: Request, rating: RatingIn, x_api_key: str = Depends(verify_api_key)):
    start = time.time()
    book = db.books.find_one({"book_id": rating.book_id})
    if not book:
        log_request(str(request.url), rating.dict(), 400, (time.time() - start)*1000, request.client.host)
        raise HTTPException(status_code=400, detail="Invalid book_id")

    db.ratings.insert_one(rating.dict())
    log_request(str(request.url), rating.dict(), 201, (time.time() - start)*1000, request.client.host)
    return rating
