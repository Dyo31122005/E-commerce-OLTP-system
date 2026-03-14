import psycopg2
from faker import Faker

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
        INSERT INTO brand (brand_name, country, created_at)
        VALUES (%s, %s, %s)
        """,
        (
            fake.company(),
            fake.country(),
            fake.date_time_this_decade()
        )
    )

conn.commit()

cur.close()
conn.close()

print("100 brands inserted")