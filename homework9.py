import requests
import psycopg2
from contextlib import contextmanager
import json

product_url = requests.get('https://dummyjson.com/products')
product_list = product_url.json()['products']

dat_base = {
    "host": "localhost",
    "port": 5432,
    "database": "cars",
    "user": "postgres",
    "password": "1111"
}


@contextmanager
def connect():
    conn = psycopg2.connect(**dat_base)
    curr = conn.cursor()
    try:
        yield conn, curr
        print('Connected!')
    except Exception as e:
        print(f"We had an error: {e}")
    finally:
        curr.close()
        conn.close()


with connect() as (conn, curr):
    create_table = """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY, 
            title VARCHAR(100) NOT NULL, 
            description VARCHAR(255) NOT NULL, 
            category VARCHAR(100) NOT NULL, 
            price NUMERIC(20, 10) NOT NULL, 
            discountPercentage NUMERIC(20, 10) NOT NULL, 
            rating NUMERIC(20, 10) NOT NULL, 
            stock INT NOT NULL, 
            properties JSONB, 
            brand TEXT DEFAULT 'no brand name', 
            sku TEXT NOT NULL, 
            weight NUMERIC(20, 10) NOT NULL
        )
    """
    curr.execute(create_table)
    conn.commit()
    print('Successfully committed!')

with connect() as (conn, curr):
    query = """
        INSERT INTO products (title, description, category, price, discountPercentage, rating, stock, properties, brand, sku, weight)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for product in product_list:
        brand = product.get('brand', 'no brand name')
        curr.execute(query, (
            product['title'], product['description'], product['category'], product['price'],
            product['discountPercentage'], product['rating'], product['stock'], json.dumps(product['tags']),
            brand, product['sku'], product['weight']
        ))
    conn.commit()
    print('Successfully inserted!')
