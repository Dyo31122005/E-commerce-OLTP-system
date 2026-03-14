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

for _ in range(100):
    cur.execute(
        """
        INSERT INTO seller (seller_name, join_date, seller_type, rating, country)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            fake.company(),
            fake.date_between(start_date="-5y", end_date="today"),
            random.choice(["Official", "Marketplace"]),
            round(random.uniform(3,5),1),
            "Vietnam"
        )
    )

conn.commit()

cur.close()
conn.close()

print("100 sellers inserted")