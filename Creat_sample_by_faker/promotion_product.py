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

# lấy promotion_id
cur.execute("SELECT promotion_id FROM promotion")
promotions = [row[0] for row in cur.fetchall()]

# lấy product_id
cur.execute("SELECT product_id FROM product")
products = [row[0] for row in cur.fetchall()]

for promotion_id in promotions:

    # mỗi promotion áp dụng cho 5-15 sản phẩm
    selected_products = random.sample(products, random.randint(5,15))

    for product_id in selected_products:

        cur.execute(
            """
            INSERT INTO promotion_product
            (promotion_id, product_id, created_at)
            VALUES (%s,%s,%s)
            """,
            (
                promotion_id,
                product_id,
                fake.date_time_this_year()
            )
        )

conn.commit()

cur.close()
conn.close()

print("Promotion-Product data inserted")