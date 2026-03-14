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

# lấy seller_id
cur.execute("SELECT seller_id FROM seller")
sellers = [row[0] for row in cur.fetchall()]

status_list = [
    "PLACED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED"
]

for _ in range(500):

    order_date = fake.date_time_between(start_date="-2y", end_date="now")

    cur.execute(
        """
        INSERT INTO "order"
        (order_date, seller_id, status, total_amount, created_at)
        VALUES (%s,%s,%s,%s,%s)
        """,
        (
            order_date,
            random.choice(sellers),
            random.choice(status_list),
            round(random.uniform(100000,5000000),2),
            order_date
        )
    )

conn.commit()

cur.close()
conn.close()

print("500 orders inserted")