from pydantic import BaseModel, Field
from typing import Optional, List


# -------------------------
# BOOK MODEL
# -------------------------
class Book(BaseModel):
    book_id: int = Field(..., description="Unique ID of the book (from CSV)")
    goodreads_book_id: Optional[int] = Field(None, description="Goodreads unique ID")
    title: str = Field(..., description="Book title")
    authors: str = Field(..., description="Author(s) of the book")
    original_publication_year: Optional[int] = Field(
        None, description="Year of first publication"
    )
    average_rating: Optional[float] = Field(
        None, ge=0, le=5, description="Average rating score"
    )
    ratings_count: Optional[int] = Field(None, description="Number of total ratings")
    image_url: Optional[str] = Field(None, description="URL to the book cover image")
    small_image_url: Optional[str] = Field(
        None, description="URL to the small thumbnail of the book cover"
    )

    class Config:
        schema_extra = {
            "example": {
                "book_id": 170,
                "goodreads_book_id": 3735293,
                "title": "Animal Farm",
                "authors": "George Orwell",
                "original_publication_year": 1945,
                "average_rating": 3.98,
                "ratings_count": 273849,
                "image_url": "https://example.com/animalfarm.jpg",
                "small_image_url": "https://example.com/animalfarm_small.jpg",
            }
        }


# -------------------------
# RATING MODELS
# -------------------------
class RatingIn(BaseModel):
    user_id: int = Field(..., description="User ID giving the rating")
    book_id: int = Field(..., description="Book ID being rated")
    rating: int = Field(..., ge=1, le=5, description="Rating value between 1 and 5")

    class Config:
        schema_extra = {
            "example": {"user_id": 2001, "book_id": 170, "rating": 5}
        }


class RatingOut(BaseModel):
    user_id: int
    book_id: int
    rating: int


# -------------------------
# PAGINATION WRAPPER
# -------------------------
class PaginatedBooks(BaseModel):
    items: List[Book]
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of books per page")
    total: int = Field(..., description="Total number of matching books")


# -------------------------
# ERROR RESPONSE MODEL
# -------------------------
class ErrorResponse(BaseModel):
    detail: str = Field(..., description="Error message")

