import os
import gzip
import psycopg2
import requests
import pandas as pd
from io import BytesIO
from bs4 import BeautifulSoup
import json
from dotenv import load_dotenv

BASE_URL = "https://amazon-reviews-2023.github.io/"

load_dotenv()

DB_NAME = os.getenv("DBNAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")



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
        return None, None


def create_meta_data_table():
    connection, cursor = connect_to_database()
    if not connection:
        return False

    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS meta_data (
            id SERIAL PRIMARY KEY,
            filename TEXT,
            main_category TEXT,
            title TEXT,
            average_rating TEXT,
            rating_number TEXT,
            features TEXT,
            description TEXT,
            price TEXT,
            images TEXT,
            videos TEXT,
            store TEXT,
            categories TEXT,
            details TEXT,
            parent_asin TEXT,
            bought_together TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_meta_parent_asin ON meta_data(parent_asin);
        CREATE INDEX IF NOT EXISTS idx_meta_filename ON meta_data(filename);
        CREATE INDEX IF NOT EXISTS idx_meta_parent_filename ON meta_data(parent_asin, filename);
        """

        cursor.execute(create_table_query)
        connection.commit()
        print("meta_data table created successfully")
        return True

    except Exception as e:
        print(f"Error creating meta_data table: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()


def insert_meta_data_to_db(meta_df):
    if meta_df.empty:
        print("No meta data to insert")
        return False

    connection, cursor = connect_to_database()
    if not connection:
        return False

    try:
        cursor.execute("DELETE FROM meta_data")
        print("Cleared existing meta_data table")

        insert_query = """
            INSERT INTO meta_data 
            (filename, main_category, title, average_rating, rating_number, 
             features, description, price, images, videos, store, categories, 
             details, parent_asin, bought_together) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data_tuples = []
        for _, row in meta_df.iterrows():
            data_tuples.append((
                str(row['filename']) if row['filename'] is not None else '',
                str(row['main_category']) if row['main_category'] is not None else '',
                str(row['title']) if row['title'] is not None else '',
                str(row['average_rating']) if row['average_rating'] is not None else '',
                str(row['rating_number']) if row['rating_number'] is not None else '',
                str(row['features']) if row['features'] is not None else '',
                str(row['description']) if row['description'] is not None else '',
                str(row['price']) if row['price'] is not None else '',
                str(row['images']) if row['images'] is not None else '',
                str(row['videos']) if row['videos'] is not None else '',
                str(row['store']) if row['store'] is not None else '',
                str(row['categories']) if row['categories'] is not None else '',
                str(row['details']) if row['details'] is not None else '',
                str(row['parent_asin']) if row['parent_asin'] is not None else '',
                str(row['bought_together']) if row['bought_together'] is not None else ''
            ))

        batch_size = 1000
        total_inserted = 0

        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            total_inserted += len(batch)

            if total_inserted % 10000 == 0:
                print(f"Inserted {total_inserted} records...")

        print(f"Successfully inserted {total_inserted} meta data records into database")
        return True

    except Exception as e:
        print(f"Error inserting meta data: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()



def create_product_dict_from_db():
    connection, cursor = connect_to_database()
    if not connection:
        return {}

    try:
        cursor.execute("SELECT DISTINCT parent_asin, filename FROM review_data")
        results = cursor.fetchall()

        product_dict = {}
        for parent_asin, filename in results:
            if filename not in product_dict:
                product_dict[filename] = set()
            product_dict[filename].add(parent_asin)

        return product_dict
    finally:
        cursor.close()
        connection.close()


user_product_dict = create_product_dict_from_db()
print(f"Files in dict: {list(user_product_dict.keys())}")


print("Creating meta_data table...")
create_meta_data_table()

def get_all_files():
    response = requests.get(BASE_URL)
    response.raise_for_status()

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


print("Fetching file list...")
files = get_all_files()
meta_files = [f for f in files if 'meta_categories' in f.lower()]

print("Meta files found:", len(meta_files))

def parse_meta_files(meta_files, user_product_dict):
    all_dfs = []
    for file_url in meta_files:
        filename = file_url.split("/")[-1].replace(".gz", "")
        filename = filename.replace('meta_', '')

        if filename not in user_product_dict:
            print(f"Skipping {filename} - no matching products in review data")
            continue

        print(f"Downloading {file_url} ...")
        print(f"Looking for {len(user_product_dict[filename])} products from {filename}")

        try:
            response = requests.get(file_url, stream=True)
            response.raise_for_status()

            data = []
            products_found = 0

            with gzip.open(BytesIO(response.content), 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        product = json.loads(line)
                        parent_asin = product.get("parent_asin")

                        if parent_asin in user_product_dict[filename]:
                            products_found += 1
                            data.append({
                                "filename": filename,
                                "main_category": product.get("main_category"),
                                "title": product.get("title"),
                                "average_rating": product.get("average_rating"),
                                "rating_number": product.get("rating_number"),
                                "features": product.get("features"),
                                "description": product.get("description"),
                                "price": product.get("price"),
                                "images": product.get("images"),
                                "videos": product.get("videos"),
                                "store": product.get("store"),
                                "categories": product.get("categories"),
                                "details": product.get("details"),
                                "parent_asin": parent_asin,
                                "bought_together": product.get("bought_together"),
                            })
                    except json.JSONDecodeError:
                        continue

            df = pd.DataFrame(data)
            print(f"{filename} loaded, shape: {df.shape}, products found: {products_found}")

            if not df.empty:
                all_dfs.append(df)

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue

    if all_dfs:
        meta_df = pd.concat(all_dfs, ignore_index=True)
        return meta_df
    else:
        return pd.DataFrame()



meta_df = parse_meta_files(meta_files, user_product_dict)
print("Meta DF shape:", meta_df.shape)

if meta_df.empty:
    print("No meta data loaded. Exiting.")
    exit()

print("=== DEBUGGING PARENT_ASIN ===")
print(f"Meta DF shape: {meta_df.shape}")
print(f"Parent_asin nulls in parsed data: {meta_df['parent_asin'].isnull().sum()}")
print(f"Parent_asin empty strings: {(meta_df['parent_asin'] == '').sum()}")

unique_nullish = meta_df[meta_df['parent_asin'].isnull() | (meta_df['parent_asin'] == '')]['parent_asin'].unique()
print(f"Unique null-ish values: {unique_nullish}")

print(f"Sample parent_asins: {meta_df['parent_asin'].dropna().head(10).tolist()}")


print("Inserting meta data into database...")
insert_success = insert_meta_data_to_db(meta_df)
if insert_success:
    print("Meta data successfully inserted into database")
else:
    print("Failed to insert meta data into database")

print("Done!")