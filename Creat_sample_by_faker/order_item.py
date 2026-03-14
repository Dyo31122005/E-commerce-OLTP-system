import psycopg2
import random

conn = psycopg2.connect(
    database="ecommerce",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# lấy orders
cur.execute('SELECT order_id FROM "order"')
orders = [row[0] for row in cur.fetchall()]

# lấy product và price
cur.execute('SELECT product_id, price FROM product')
products = cur.fetchall()

for order_id in orders:

    # mỗi order có 1-5 items
    for _ in range(random.randint(1,5)):

        product_id, price = random.choice(products)

        quantity = random.randint(1,3)

        subtotal = quantity * price

        cur.execute(
            """
            INSERT INTO order_item
            (order_id, product_id, quantity, unit_price, subtotal)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (
                order_id,
                product_id,
                quantity,
                round(price,2),
                round(subtotal,2)
            )
        )

conn.commit()

cur.close()
conn.close()

print("Order items inserted")