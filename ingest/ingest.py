import pandas as pd
from pymongo import MongoClient
from pymongo import ReplaceOne

client = MongoClient( "mongodb://localhost:27017/")
db = client["goodbooks"]
samples = {
    "books": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/books.csv",
    "ratings": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/ratings.csv",
    "tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/tags.csv",
    "book_tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/book_tags.csv",
    "to_read": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/to_read.csv",
}

def read_csv(path_or_url):
    return pd.read_csv(path_or_url)

def chunked(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]


def bulk_upsert(df, collection, key_fields, transform=lambda r: r, chunk_size=1000):
    """
    Bulk upsert documents from a DataFrame into a MongoDB collection.

    Args:
        df (pd.DataFrame): DataFrame to upsert.
        collection: MongoDB collection object.
        key_fields (list): List of fields to use as unique keys.
        transform (callable): Function to transform each row dict before upsert.
        chunk_size (int): Number of upserts per bulk_write.
    """
    docs = df.to_dict(orient="records")
    ops = []
    for d in docs:
        filt = {k: d[k] for k in key_fields}
        ops.append(ReplaceOne(filt, transform(d), upsert=True))
    for chunk in chunked(ops, chunk_size):
        collection.bulk_write(chunk)

def clean_books_row(r):
    # Ensure book_id is int
    r['book_id'] = int(r['book_id'])
    # Handle average_rating NaN
    if r.get('average_rating') is None or (isinstance(r.get('average_rating'), float) and pd.isna(r.get('average_rating'))):
        r['average_rating'] = 0.0
    # Handle integer columns
    for int_col in ['ratings_count', 'original_publication_year', 'goodreads_book_id']:
        if r.get(int_col) is None or pd.isna(r.get(int_col)):
            r[int_col] = None
        else:
            try:
                r[int_col] = int(r[int_col])
            except Exception:
                r[int_col] = None
    return r

import argparse

def main():
    parser = argparse.ArgumentParser(description="Ingest Goodbooks sample data into MongoDB.")
    parser.add_argument("--source", choices=["sample", "full"], default="sample", help="Choose data source: sample or full")
    args = parser.parse_args()

    base = samples if args.source == "sample" else None
    if base is None:
        # Add full dataset URLs or local paths here
        base = {
            "books": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/books.csv",
            "ratings": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/ratings.csv",
            "tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/tags.csv",
            "book_tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/book_tags.csv",
            "to_read": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/to_read.csv",
        }

    print("Reading books...")
    books_df = read_csv(base['books'])
    books_df = books_df.fillna("")
    bulk_upsert(books_df, db.books, key_fields=['book_id'], transform=clean_books_row)
    print("Books done.")

    print("Reading ratings...")
    ratings_df = read_csv(base['ratings'])
    ratings_df = ratings_df.fillna(0)
    bulk_upsert(ratings_df, db.ratings, key_fields=['user_id', 'book_id'])
    print("Ratings done.")

    print("Reading tags...")
    tags_df = read_csv(base['tags'])
    tags_df = tags_df.fillna("")
    bulk_upsert(tags_df, db.tags, key_fields=['tag_id'])
    print("Tags done.")

    print("Reading book_tags...")
    book_tags_df = read_csv(base['book_tags'])
    book_tags_df = book_tags_df.fillna(0)
    bulk_upsert(book_tags_df, db.book_tags, key_fields=['goodreads_book_id', 'tag_id'])
    print("book_tags done.")

    print("Reading to_read...")
    to_read_df = read_csv(base['to_read'])
    to_read_df = to_read_df.fillna(0)
    bulk_upsert(to_read_df, db.to_read, key_fields=['user_id', 'book_id'])
    print("to_read done.")

if __name__ == "__main__":
    main()