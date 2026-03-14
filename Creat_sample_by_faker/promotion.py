import psycopg2
from faker import Faker
import random
from datetime import timedelta

fake = Faker()

conn = psycopg2.connect(
    database="ecommerce",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

promotion_types = ["product", "category", "seller", "flash_sale"]
discount_types = ["percentage", "fixed_amount"]

promotion_names = [
    "9.9 Mega Sale",
    "11.11 Super Sale",
    "Black Friday",
    "Flash Deal",
    "Holiday Sale",
    "Summer Promotion"
]

for _ in range(50):

    start_date = fake.date_between(start_date="-2y", end_date="today")
    end_date = start_date + timedelta(days=random.randint(30,50))

    discount_type = random.choice(discount_types)

    if discount_type == "percentage":
        discount_value = random.randint(5,50)
    else:
        discount_value = random.randint(10000,200000)

    cur.execute(
        """
        INSERT INTO promotion
        (promotion_name, promotion_type, discount_type, discount_value, start_date, end_date)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (
            random.choice(promotion_names),
            random.choice(promotion_types),
            discount_type,
            discount_value,
            start_date,
            end_date
        )
    )

conn.commit()

cur.close()
conn.close()

print("50 promotions inserted")