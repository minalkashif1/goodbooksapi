from pymongo import MongoClient, ASCENDING, DESCENDING

# --- MongoDB connection ---
client = MongoClient("mongodb://localhost:27017/")
db = client["goodbooks"]

print("🔧 Creating indexes...")

# --- Books ---
db.books.create_index([("title", "text"), ("authors", "text")], name="title_authors_text")
db.books.create_index([("average_rating", DESCENDING)], name="avg_rating_desc")
db.books.create_index([("book_id", ASCENDING)], unique=True, name="book_id_unique")

# --- Ratings ---
db.ratings.create_index([("book_id", ASCENDING)], name="book_id_idx")
db.ratings.create_index([("user_id", ASCENDING), ("book_id", ASCENDING)], unique=True, name="user_book_unique")

# --- Tags ---
db.tags.create_index([("tag_id", ASCENDING)], unique=True, name="tag_id_unique")
db.tags.create_index([("tag_name", ASCENDING)], unique=True, name="tag_name_unique")

# --- Book tags ---
db.book_tags.create_index([("tag_id", ASCENDING)], name="tag_id_idx")
db.book_tags.create_index([("goodreads_book_id", ASCENDING)], name="gr_book_id_idx")

# --- To read ---
db.to_read.create_index([("user_id", ASCENDING), ("book_id", ASCENDING)], unique=True, name="to_read_unique")

print(" Indexes created successfully.")
