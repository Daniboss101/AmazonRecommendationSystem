import gzip
import os
import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_NAME = os.getenv("DBNAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

BASE_URL = "https://amazon-reviews-2023.github.io/"
DATA_DIR = "amazon_data"
os.makedirs(DATA_DIR, exist_ok=True)

MAX_REVIEWS = 50000
MAX_ROWS_TO_READ = 300000
start_date = datetime.datetime(2020, 1, 1).timestamp() * 1000


def connect_to_database():
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = connection.cursor()
        print("Connected to PostgreSQL")
        return connection, cursor
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)


def get_all_files():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        print("Index page loaded")
    else:
        print("Index page not loaded")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")
    table = tables[1]
    links = []

    for a in table.find_all("a"):
        href = a.get("href")
        if href:
            if href.startswith("http"):
                links.append(href)
            else:
                links.append(BASE_URL + href)
    return links


def get_existing_users(cursor):
    try:
        cursor.execute("SELECT DISTINCT user_id FROM review_data ")
        existing_users = [row[0] for row in cursor.fetchall()]
        return existing_users
    except psycopg2.Error:
        return []


def get_completed_categories(cursor):
    try:
        cursor.execute("SELECT DISTINCT filename FROM review_data")
        completed = [row[0] for row in cursor.fetchall()]
        return completed
    except psycopg2.Error:
        return []


def save_category_data(df, connection, cursor):
    df_renamed = df.rename(columns={
        'text': 'review_text',
        'timestamp': 'review_timestamp',
        'helpful_votes': 'helpful_vote'
    })

    insert_query = """
        INSERT INTO review_data 
        (user_id, parent_asin, asin, rating, title, review_text, images, 
         review_timestamp, verified_purchase, helpful_vote, filename) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    data_tuples = []
    for _, row in df_renamed.iterrows():
        images_value = row['images']
        if isinstance(images_value, (dict, list)):
            images_json = json.dumps(images_value)
        elif images_value is None:
            images_json = None
        else:
            images_json = str(images_value)

        data_tuples.append((
            row['user_id'],
            row['parent_asin'],
            row['asin'],
            row['rating'],
            row['title'],
            row['review_text'],
            images_json,
            row['review_timestamp'],
            row['verified_purchase'],
            row['helpful_vote'],
            row['filename']
        ))

    try:
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
        print(f"Successfully inserted {len(data_tuples)} records")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()


def parse_review_files(review_files, connection, cursor):
    all_dfs = []
    user_product_dict = {}
    global_users = get_existing_users(cursor)
    completed_filenames = get_completed_categories(cursor)

    for file_url in review_files:
        filename = file_url.split("/")[-1].replace(".gz", "")

        if filename in completed_filenames:
            print(f"Skipping {filename} - already processed")
            continue

        print(f"Streaming {file_url} ...")
        print(f"Previous users tracked: {len(global_users)}")

        response = requests.get(file_url, stream=True)
        response.raise_for_status()

        priority_data = []
        other_data = []
        file_users = set()
        rows_read = 0
        lines_processed = 0

        print(f"Reading first {MAX_ROWS_TO_READ} rows from file...")

        decompressor = gzip.GzipFile(fileobj=response.raw)

        buffer = ""
        try:
            for chunk in iter(lambda: decompressor.read(8192), b''):
                if rows_read >= MAX_ROWS_TO_READ:
                    break

                buffer += chunk.decode('utf-8', errors='ignore')

                while '\n' in buffer and rows_read < MAX_ROWS_TO_READ:
                    line, buffer = buffer.split('\n', 1)

                    if line.strip():
                        lines_processed += 1
                        try:
                            review = json.loads(line)
                            ts = review.get("timestamp", 0)
                            user_id = review.get("user_id")

                            if ts >= start_date:
                                rows_read += 1

                                review_data = {
                                    "user_id": user_id,
                                    "parent_asin": review.get("parent_asin"),
                                    "asin": review.get("asin"),
                                    "rating": review.get("rating"),
                                    "title": review.get("title"),
                                    "text": review.get("text"),
                                    "images": review.get("images"),
                                    "timestamp": ts,
                                    "verified_purchase": review.get("verified_purchase"),
                                    "helpful_votes": review.get("helpful_vote", 0),
                                    "filename": filename
                                }

                                if user_id in global_users:
                                    priority_data.append(review_data)
                                    file_users.add(user_id)
                                else:
                                    other_data.append(review_data)

                                if rows_read % 50000 == 0:
                                    print(f"Processed {rows_read} valid rows so far...")

                        except json.JSONDecodeError:
                            continue
        finally:
            decompressor.close()
            response.close()

        print(f"Read {rows_read} valid rows from {lines_processed} total lines")

        final_data = priority_data.copy()
        remaining_slots = MAX_REVIEWS - len(final_data)
        print(f"File: {filename} Existing users: {len(final_data)}")

        if remaining_slots > 0:
            for review_data in other_data:
                if len(final_data) >= MAX_REVIEWS:
                    break
                final_data.append(review_data)
                file_users.add(review_data["user_id"])

        global_users.extend([user for user in file_users if user not in global_users])

        user_product_dict[filename] = set()
        for review_data in final_data:
            user_product_dict[filename].add(review_data["parent_asin"])

        df = pd.DataFrame(final_data)
        print(f"{filename} loaded, shape: {df.shape}")
        print(f"Priority users included: {len(priority_data)}")
        print(f"New users added: {len(final_data) - len(priority_data)}")
        print(f"Total global users: {len(global_users)}")
        print(df.head())

        if not df.empty:
            save_category_data(df, connection, cursor)
            print(f"Saved {filename} to database")

        all_dfs.append(df)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True), user_product_dict
    else:
        return pd.DataFrame(), user_product_dict


def main():
    print("Starting main function...")

    print("Getting all files...")
    files = get_all_files()
    print(f"Found {len(files)} total files")

    print("Connecting to database...")
    connection, cursor = connect_to_database()

    if not connection:
        print("Failed to connect to database - exiting")
        return

    try:
        print("Filtering for review files...")
        review_files = [f for f in files if 'review_categories' in f.lower()]
        print(f"Found {len(review_files)} review files:")
        for f in review_files:
            print(f"  - {f}")

        if not review_files:
            print("No review files found! Check the filtering logic.")
            return

        print("Starting to parse review files...")
        review_df, user_product_dict = parse_review_files(review_files, connection, cursor)

        print(f'Review df shape: {review_df.shape}')
        if not review_df.empty:
            print(f'Review df head:\n{review_df.head()}')

            review_df.to_parquet("amazon_review2_only.parquet", index=False, engine='pyarrow')
            print("review_df.to_parquet saved")
        else:
            print("No new data to process")

    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed")


if __name__ == "__main__":
    main()