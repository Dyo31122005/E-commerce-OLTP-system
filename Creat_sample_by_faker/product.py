import psycopg2
from faker import Faker
import random

fake = Faker()

conn = psycopg2.connect(
    database="ecommerce",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# lấy danh sách id từ các bảng
cur.execute("SELECT category_id FROM category")
categories = [row[0] for row in cur.fetchall()]

cur.execute("SELECT brand_id FROM brand")
brands = [row[0] for row in cur.fetchall()]

cur.execute("SELECT seller_id FROM seller")
sellers = [row[0] for row in cur.fetchall()]

for _ in range(500):   # tạo 500 products

    price = random.uniform(100000, 50000000)
    discount_price = price * random.uniform(0.7,1.0)

    cur.execute(
        """
        INSERT INTO product (
            product_name,
            category_id,
            brand_id,
            seller_id,
            price,
            discount_price,
            stock_qty,
            rating,
            created_at,
            is_active
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            fake.catch_phrase(),
            random.choice(categories),
            random.choice(brands),
            random.choice(sellers),
            round(price,2),
            round(discount_price,2),
            random.randint(0,500),
            round(random.uniform(3,5),1),
            fake.date_time_between(start_date="-3y", end_date="now"),
            random.choice([True, False])
        )
    )

conn.commit()

cur.close()
conn.close()

print("500 products inserted")