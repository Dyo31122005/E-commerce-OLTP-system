import psycopg2
from faker import Faker
import random

fake = Faker()

# connect PostgreSQL
conn = psycopg2.connect(
    database="ecommerce",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# category chính (level 1)
main_categories = [
    "Electronics",
    "Fashion",
    "Home & Kitchen",
    "Sports",
    "Books"
]

main_ids = []

# insert main category
for name in main_categories:
    cur.execute(
        """
        INSERT INTO category (category_name, parent_category_id, level, created_at)
        VALUES (%s, %s, %s, %s)
        RETURNING category_id
        """,
        (name, None, 1, fake.date_time_this_year())
    )

    cid = cur.fetchone()[0]
    main_ids.append(cid)

# subcategories (level 2)
subcategories = {
    "Electronics": ["Mobile Phones", "Laptops", "Tablets"],
    "Fashion": ["Men Clothing", "Women Clothing"],
    "Home & Kitchen": ["Furniture", "Kitchen Tools"],
    "Sports": ["Fitness Equipment", "Outdoor Gear"],
    "Books": ["Fiction", "Education"]
}

# insert subcategories
for i, main in enumerate(main_categories):
    parent_id = main_ids[i]

    for sub in subcategories[main]:
        cur.execute(
            """
            INSERT INTO category (category_name, parent_category_id, level, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (sub, parent_id, 2, fake.date_time_this_year())
        )

conn.commit()

cur.close()
conn.close()

print("Categories inserted successfully")