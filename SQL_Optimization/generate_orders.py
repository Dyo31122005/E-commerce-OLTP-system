"""
Project 04 - Generate order & order_item data for PostgreSQL
Target: 2.5M - 3M orders, ~7.5M - 12M order_items
Strategy: Generate in batches → write to CSV → COPY INTO PostgreSQL (fastest method)
"""

import csv
import os
import random
import time
from datetime import datetime, timedelta
from io import StringIO

import psycopg2
import psycopg2.extras
from faker import Faker

fake = Faker()

# ──────────────────────────────────────────
# CONFIG - chỉnh lại thông tin kết nối DB
# ──────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce",
    "user": "postgres",
    "password": "123456",
}

TOTAL_ORDERS = 2_750_000       # target số orders (2.5M - 3M)
BATCH_SIZE   = 50_000          # số orders mỗi batch
ORDER_DATE_START = datetime(2025, 8, 1)
ORDER_DATE_END   = datetime(2025, 10, 31, 23, 59, 59)

STATUS_CHOICES = [
    ("PLACED",    0.05),
    ("PAID",      0.04),
    ("SHIPPED",   0.11),
    ("DELIVERED", 0.70),
    ("CANCELLED", 0.07),
    ("RETURNED",  0.03),
]
STATUS_POPULATION = [s for s, w in STATUS_CHOICES]
STATUS_WEIGHTS    = [w for s, w in STATUS_CHOICES]


# ──────────────────────────────────────────
# HELPER
# ──────────────────────────────────────────
def random_timestamp(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def weighted_status() -> str:
    return random.choices(STATUS_POPULATION, STATUS_WEIGHTS, k=1)[0]


# ──────────────────────────────────────────
# STEP 1: Load reference data from DB
# ──────────────────────────────────────────
def load_reference_data(conn) -> dict[int, list[tuple]]:
    """
    Trả về dict: seller_id -> list of (product_id, discount_price)
    Chỉ lấy product is_active = TRUE để đảm bảo tính hợp lệ.
    """
    print("📥 Loading product reference data from DB...")
    cur = conn.cursor()
    cur.execute("""
        SELECT seller_id, product_id, discount_price
        FROM product
        WHERE is_active = TRUE
        ORDER BY seller_id
    """)
    rows = cur.fetchall()
    cur.close()

    seller_products: dict[int, list[tuple]] = {}
    for seller_id, product_id, discount_price in rows:
        seller_products.setdefault(seller_id, []).append(
            (product_id, float(discount_price))
        )

    print(f"   ✅ Loaded {len(rows):,} active products across {len(seller_products):,} sellers")
    return seller_products


def load_seller_ids(conn) -> list[int]:
    """Trả về danh sách seller_id có ít nhất 2 sản phẩm (để chọn 2-4 items/order)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT seller_id
        FROM product
        WHERE is_active = TRUE
        GROUP BY seller_id
        HAVING COUNT(*) >= 2
    """)
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    print(f"   ✅ {len(ids):,} eligible sellers (≥2 active products)")
    return ids


# ──────────────────────────────────────────
# STEP 2: Create tables (nếu chưa có)
# ──────────────────────────────────────────
CREATE_ORDER_TABLE = """
CREATE TABLE IF NOT EXISTS orders (
    order_id     SERIAL PRIMARY KEY,
    order_date   TIMESTAMP     NOT NULL,
    seller_id    INT           NOT NULL REFERENCES seller(seller_id),
    status       VARCHAR(20)   NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    created_at   TIMESTAMP     NOT NULL DEFAULT NOW()
);
"""

CREATE_ORDER_ITEM_TABLE = """
CREATE TABLE IF NOT EXISTS order_item (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      INT            NOT NULL REFERENCES orders(order_id),
    product_id    INT            NOT NULL REFERENCES product(product_id),
    order_date    TIMESTAMP      NOT NULL,
    quantity      INT            NOT NULL,
    unit_price    NUMERIC(10,2)  NOT NULL,
    subtotal      NUMERIC(12,2)  NOT NULL,
    created_at    TIMESTAMP      NOT NULL DEFAULT NOW()
);
"""

# Index giúp query nhanh hơn sau khi insert
CREATE_INDEXES = [
    'CREATE INDEX IF NOT EXISTS idx_order_seller    ON orders(seller_id);',
    'CREATE INDEX IF NOT EXISTS idx_order_date      ON orders(order_date);',
    'CREATE INDEX IF NOT EXISTS idx_order_status    ON orders(status);',
    'CREATE INDEX IF NOT EXISTS idx_oi_order_id     ON order_item(order_id);',
    'CREATE INDEX IF NOT EXISTS idx_oi_product_id   ON order_item(product_id);',
    'CREATE INDEX IF NOT EXISTS idx_oi_order_date   ON order_item(order_date);',
]


def create_tables(conn):
    print("🛠  Dropping old tables and recreating...")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS order_item CASCADE;")
    cur.execute("DROP TABLE IF EXISTS orders CASCADE;")
    cur.execute(CREATE_ORDER_TABLE)
    cur.execute(CREATE_ORDER_ITEM_TABLE)
    conn.commit()
    cur.close()
    print("   ✅ Tables ready")


def create_indexes(conn):
    print("🔍 Creating indexes...")
    cur = conn.cursor()
    for sql in CREATE_INDEXES:
        cur.execute(sql)
    conn.commit()
    cur.close()
    print("   ✅ Indexes created")


# ──────────────────────────────────────────
# STEP 3: Generate + COPY batch
# ──────────────────────────────────────────
def copy_from_stringio(conn, table: str, columns: list[str], rows: list[list]):
    """Dùng COPY để bulk insert - nhanh hơn executemany ~10-20x."""
    buf = StringIO()
    writer = csv.writer(buf, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        writer.writerow(row)
    buf.seek(0)

    cur = conn.cursor()
    cur.copy_from(buf, table, sep="\t", columns=columns, null="")
    conn.commit()
    cur.close()


def generate_and_insert(conn, seller_ids: list[int], seller_products: dict[int, list[tuple]]):
    """
    Sinh dữ liệu theo batch:
      - Mỗi batch: BATCH_SIZE orders
      - Mỗi order: 2-4 items từ cùng seller
      - COPY vào DB sau mỗi batch
    """
    total_orders_done = 0
    total_items_done  = 0
    start_time        = time.time()

    # Lấy order_id bắt đầu (để tránh conflict nếu chạy lại)
    cur = conn.cursor()
    cur.execute('SELECT COALESCE(MAX(order_id), 0) FROM orders')
    order_id_counter = cur.fetchone()[0] + 1
    cur.close()

    batches = (TOTAL_ORDERS + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num in range(1, batches + 1):
        current_batch_size = min(BATCH_SIZE, TOTAL_ORDERS - total_orders_done)
        if current_batch_size <= 0:
            break

        order_rows = []       # rows cho bảng "order"
        item_rows  = []       # rows cho bảng order_item

        for _ in range(current_batch_size):
            # Chọn seller ngẫu nhiên
            seller_id = random.choice(seller_ids)
            products  = seller_products[seller_id]

            # Chọn 2-4 sản phẩm không trùng
            num_items = random.randint(2, min(4, len(products)))
            chosen    = random.sample(products, num_items)

            order_date = random_timestamp(ORDER_DATE_START, ORDER_DATE_END)
            status     = weighted_status()
            total_amount = 0.0

            for product_id, unit_price in chosen:
                qty      = random.randint(1, 5)
                subtotal = round(qty * unit_price, 2)
                total_amount += subtotal

                item_rows.append([
                    order_id_counter,   # order_id
                    product_id,
                    order_date,         # order_date
                    qty,                # quantity
                    round(unit_price, 2),
                    subtotal,
                    order_date,         # created_at
                ])

            order_rows.append([
                order_date,
                seller_id,
                status,
                round(total_amount, 2),
                order_date,             # created_at
            ])

            order_id_counter += 1

        # COPY orders
        copy_from_stringio(
            conn,
            "orders",
            ["order_date", "seller_id", "status", "total_amount", "created_at"],
            order_rows,
        )

        # COPY order_items
        copy_from_stringio(
            conn,
            "order_item",
            ["order_id", "product_id", "order_date", "quantity",
             "unit_price", "subtotal", "created_at"],
            item_rows,
        )

        total_orders_done += current_batch_size
        total_items_done  += len(item_rows)
        elapsed = time.time() - start_time
        speed   = total_orders_done / elapsed if elapsed > 0 else 0

        print(
            f"   Batch {batch_num:>3}/{batches} | "
            f"Orders: {total_orders_done:>9,} | "
            f"Items: {total_items_done:>11,} | "
            f"Speed: {speed:>8,.0f} orders/s | "
            f"Elapsed: {elapsed:>6.1f}s"
        )

    return total_orders_done, total_items_done


# ──────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────
def main():
    print("=" * 65)
    print("  Project 04 — Order Data Generator (PostgreSQL + COPY)")
    print("=" * 65)

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        # 1. Tạo bảng
        create_tables(conn)

        # 2. Load reference data
        seller_products = load_reference_data(conn)
        seller_ids      = load_seller_ids(conn)

        if not seller_ids:
            print("❌ No eligible sellers found. Please check product table.")
            return

        # 3. Generate + insert
        print(f"\n🚀 Generating {TOTAL_ORDERS:,} orders in batches of {BATCH_SIZE:,}...")
        t0 = time.time()
        total_orders, total_items = generate_and_insert(conn, seller_ids, seller_products)
        elapsed = time.time() - t0

        # 4. Tạo indexes sau khi insert (nhanh hơn tạo trước)
        create_indexes(conn)

        print("\n" + "=" * 65)
        print(f"  ✅ DONE!")
        print(f"     Orders inserted : {total_orders:>12,}")
        print(f"     Items inserted  : {total_items:>12,}")
        print(f"     Total time      : {elapsed:>10.1f}s  ({elapsed/60:.1f} min)")
        print(f"     Avg speed       : {total_orders/elapsed:>10,.0f} orders/s")
        print("=" * 65)

    finally:
        conn.close()


if __name__ == "__main__":
    main()