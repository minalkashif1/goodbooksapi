from pydantic import BaseModel, Field
from typing import Optional, List

class Book(BaseModel):
    book_id: int = Field(..., description="Unique ID of the book")
    title: str
    authors: str
    average_rating: float
    ratings_count: Optional[int]

class RatingIn(BaseModel):
    rating: int = Field(..., ge=1, le=5, description="Rating from 1 to 5")

class RatingOut(BaseModel):
    user_id: int
    book_id: int
    rating: int

class ErrorResponse(BaseModel):
    detail: str
