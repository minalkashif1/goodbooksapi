from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson import ObjectId

app = FastAPI()

client = MongoClient("mongodb://localhost:27017/")
db = client["goodbooks"]

@app.get("/")
def home():
    return {"message": "Welcome to the Goodbooks API"}

@app.get("/books/{book_id}")
def get_book(book_id: int):
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    book["_id"] = str(book["_id"])
    return book

@app.get("/search")
def search_books(q: str, limit: int = 10):
    books = list(db.books.find({"title": {"$regex": q, "$options": "i"}}).limit(limit))
    for b in books:
        b["_id"] = str(b["_id"])
    return books

