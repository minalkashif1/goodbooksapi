import argparse
import pandas as pd
from pymongo import MongoClient, ReplaceOne

# --- MongoDB connection ---
client = MongoClient("mongodb://localhost:27017/")
db = client["goodbooks"]

# --- CSV sources (sample URLs) ---
SAMPLE_URLS = {
    "books": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/books.csv",
    "ratings": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/ratings.csv",
    "tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/tags.csv",
    "book_tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/book_tags.csv",
    "to_read": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/to_read.csv",
}

# --- Helper functions ---
def read_csv(path_or_url):
    """Reads a CSV file or URL into a pandas DataFrame."""
    return pd.read_csv(path_or_url)

def chunked(iterable, n):
    """Yield successive n-sized chunks from an iterable."""
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

def bulk_upsert(df, collection, key_fields, transform=lambda r: r, chunk_size=1000):
    """
    Perform bulk upsert operations from a DataFrame into a MongoDB collection.
    """
    docs = df.to_dict(orient="records")
    ops = []
    for d in docs:
        filt = {k: d[k] for k in key_fields}
        ops.append(ReplaceOne(filt, transform(d), upsert=True))
    for chunk in chunked(ops, chunk_size):
        collection.bulk_write(chunk)

def clean_books_row(row):
    """Cleans and normalizes a single row of book data."""
    # Ensure book_id is int
    row["book_id"] = int(row["book_id"])

    # Replace NaN average_rating with 0.0
    if pd.isna(row.get("average_rating")):
        row["average_rating"] = 0.0

    # Convert integer columns
    for col in ["ratings_count", "original_publication_year", "goodreads_book_id"]:
        if pd.isna(row.get(col)):
            row[col] = None
        else:
            try:
                row[col] = int(row[col])
            except Exception:
                row[col] = None

    return row

# --- Main ingestion logic ---
def main():
    parser = argparse.ArgumentParser(description="Ingest Goodbooks sample data into MongoDB.")
    parser.add_argument("--source", choices=["sample", "full"], default="sample", help="Choose dataset source (sample/full)")
    args = parser.parse_args()

    base = SAMPLE_URLS if args.source == "sample" else SAMPLE_URLS  # full URLs could go here later

    print(" Ingesting Goodbooks data...")

    print("→ Loading books...")
    books_df = read_csv(base["books"]).fillna("")
    bulk_upsert(books_df, db.books, key_fields=["book_id"], transform=clean_books_row)
    print(" Books inserted/updated.")

    print("→ Loading ratings...")
    ratings_df = read_csv(base["ratings"]).fillna(0)
    bulk_upsert(ratings_df, db.ratings, key_fields=["user_id", "book_id"])
    print(" Ratings inserted/updated.")

    print("→ Loading tags...")
    tags_df = read_csv(base["tags"]).fillna("")
    bulk_upsert(tags_df, db.tags, key_fields=["tag_id"])
    print(" Tags inserted/updated.")

    print("→ Loading book_tags...")
    book_tags_df = read_csv(base["book_tags"]).fillna(0)
    bulk_upsert(book_tags_df, db.book_tags, key_fields=["goodreads_book_id", "tag_id"])
    print("Book tags inserted/updated.")

    print("→ Loading to_read...")
    to_read_df = read_csv(base["to_read"]).fillna(0)
    bulk_upsert(to_read_df, db.to_read, key_fields=["user_id", "book_id"])
    print(" To-read inserted/updated.")

    print("\n Data ingestion completed successfully!")

if __name__ == "__main__":
    main()
