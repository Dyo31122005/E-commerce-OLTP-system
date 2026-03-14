# E-commerce OLTP Synthetic Dataset Generator

## Overview

This project generates a synthetic dataset for an **E-commerce OLTP system** using **Python and Faker**.
The generated data is inserted into a **PostgreSQL database** and follows a relational schema similar to a real e-commerce platform.

The goal of the project is to simulate transactional data such as products, orders, sellers, and promotions for database practice, analytics, or testing.

---

## Tech Stack

* Python 3
* Faker
* PostgreSQL
* Poetry (dependency management)

---

## Database Schema

The dataset simulates an e-commerce platform with the following tables:

| Table             | Description                             |
| ----------------- | --------------------------------------- |
| brand             | Product brands                          |
| category          | Product categories with hierarchy       |
| seller            | Sellers or shops                        |
| product           | Products sold on the platform           |
| order             | Customer orders                         |
| order_item        | Items included in orders                |
| promotion         | Promotional campaigns                   |
| promotion_product | Mapping between promotions and products |

Relationships include:

* One **brand → many products**
* One **category → many products**
* One **seller → many products**
* One **order → many order items**
* One **product → many order items**
* One **promotion → many products**

---

## Project Structure

```
project_3
│
├── pyproject.toml
├── README.md
│
├── scripts
│   ├── brand.py
│   ├── category.py
│   ├── seller.py
│   ├── product.py
│   ├── order.py
│   ├── order_item.py
│   ├── promotion.py
│   └── promotion_product.py
```

Each script generates and inserts synthetic data into its corresponding table.

---

## Installation

Clone the repository or navigate to the project folder, then install dependencies using Poetry.

```
poetry install
```

Required dependencies:

* Faker
* psycopg2

---

## Database Setup

Make sure PostgreSQL is running and create a database.

Example:

```
CREATE DATABASE ecommerce;
```

Update database connection settings inside the Python scripts:

```
database="ecommerce"
user="postgres"
password="your_password"
host="localhost"
port="5432"
```

---

## Running Data Generation

Run each script using Poetry.

Example:

```
poetry run python scripts/brand.py
poetry run python scripts/category.py
poetry run python scripts/seller.py
poetry run python scripts/product.py
poetry run python scripts/order.py
poetry run python scripts/order_item.py
poetry run python scripts/promotion.py
poetry run python scripts/promotion_product.py
```

These scripts will generate synthetic records and insert them into the database.

---

## Example Use Cases

This dataset can be used for:

* SQL practice
* Database design learning
* Data analytics exercises
* BI dashboards
* Query optimization experiments

---

