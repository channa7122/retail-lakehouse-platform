"""Production-Grade Synthetic Retail Data Generator.

Generates realistic, semi-dirty data with injected duplicates, null values,
inconsistent date formats, and negative values to simulate real-world raw ingestion.
"""

from datetime import datetime
import json
from pathlib import Path
import random
from faker import Faker
import numpy as np
import pandas as pd

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

RAW_DATA_DIR = Path("data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


def generate_stores(num_stores: int = 50) -> pd.DataFrame:
    """Generate store catalog with whitespace and case inconsistencies."""
    store_types = ["Supercenter", "Express", "Hypermarket", "Outlet"]
    regions = ["North", "South", "East", "West", "Central"]

    records = []
    for i in range(1, num_stores + 1):
        # Inject whitespace / casing noise into 10% of records
        name = f"Retail Store {i}"
        if random.random() < 0.1:
            name = f"  retail store {i}  "

        records.append(
            {
                "store_id": f"STR_{i:03d}",
                "store_name": name,
                "city": fake.city(),
                "state": fake.state_abbr(),
                "region": random.choice(regions),
                "store_type": random.choice(store_types),
                "opened_date": fake.date_between(
                    start_date="-15y", end_date="-1y"
                ).isoformat(),
            }
        )
    return pd.DataFrame(records)


def generate_products(num_products: int = 500) -> pd.DataFrame:
    """Generate product master with edge cases and missing costs."""
    categories = {
        "Electronics": ["Laptops", "Headphones", "Tablets", "Smartphones", "Smartwatches"],
        "Home & Kitchen": ["Cookware", "Small Appliances", "Furniture", "Bedding"],
        "Apparel": ["Men's Wear", "Women's Wear", "Footwear", "Accessories"],
        "Groceries": ["Beverages", "Snacks", "Pantry Essentials", "Dairy"],
    }

    records = []
    for i in range(1, num_products + 1):
        cat = random.choice(list(categories.keys()))
        sub_cat = random.choice(categories[cat])
        cost = round(random.uniform(5.0, 500.0), 2)
        price = round(cost * random.uniform(1.15, 1.8), 2)

        # Inject null costs in 3% of products
        final_cost = None if random.random() < 0.03 else cost

        records.append(
            {
                "product_id": f"PRD_{i:04d}",
                "product_name": f"{fake.word().capitalize()} {sub_cat[:-1]}",
                "category": cat.lower() if random.random() < 0.05 else cat,
                "sub_category": sub_cat,
                "unit_cost": final_cost,
                "retail_price": price,
            }
        )
    return pd.DataFrame(records)


def generate_customers(num_customers: int = 5000) -> pd.DataFrame:
    """Generate customers with duplicate emails and missing loyalty tiers."""
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    records = []
    for i in range(1, num_customers + 1):
        email = fake.email()
        # Inject invalid emails in 2% of records
        if random.random() < 0.02:
            email = "invalid_email_at_domain"

        records.append(
            {
                "customer_id": f"CUST_{i:05d}",
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": email,
                "city": fake.city(),
                "state": fake.state_abbr(),
                "loyalty_tier": None if random.random() < 0.05 else random.choice(tiers),
                "created_at": fake.date_between(
                    start_date="-4y", end_date="today"
                ).isoformat(),
            }
        )
    return pd.DataFrame(records)


def generate_orders_batch(
    customers_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    products_df: pd.DataFrame,
    num_orders: int = 50000,
) -> list[dict]:
    """Generate high-volume orders with deliberate duplicates and dirty records."""
    orders = []
    statuses = ["Completed", "Completed", "Completed", "Pending", "Cancelled", "Refunded"]
    pay_methods = ["Credit Card", "Debit Card", "PayPal", "Cash", "Apple Pay"]

    customer_ids = customers_df["customer_id"].tolist()
    store_ids = stores_df["store_id"].tolist()
    product_records = products_df[["product_id", "retail_price"]].to_dict("records")

    for i in range(1, num_orders + 1):
        # Inconsistent date format injection
        dt = fake.date_time_between(start_date="-180d", end_date="now")
        rand_fmt = random.random()
        if rand_fmt < 0.85:
            timestamp_str = dt.isoformat()
        elif rand_fmt < 0.95:
            timestamp_str = dt.strftime("%Y/%m/%d %H:%M:%S")
        else:
            timestamp_str = dt.strftime("%d-%m-%Y")

        num_items = random.randint(1, 4)
        chosen_prods = random.sample(product_records, num_items)
        items = []

        for prod in chosen_prods:
            qty = random.randint(1, 5)
            # Inject negative quantity anomalies in 0.5% of items
            if random.random() < 0.005:
                qty = -1

            items.append(
                {
                    "order_item_id": f"ITEM_{i:06d}_{prod['product_id']}",
                    "product_id": prod["product_id"],
                    "quantity": qty,
                    "unit_price": float(prod["retail_price"]),
                    "discount_amount": 0.0 if random.random() < 0.7 else round(float(prod["retail_price"]) * 0.1, 2),
                }
            )

        order_record = {
            "order_id": f"ORD_{i:07d}",
            "customer_id": random.choice(customer_ids),
            "store_id": random.choice(store_ids),
            "order_timestamp": timestamp_str,
            "order_status": random.choice(statuses),
            "payment_method": random.choice(pay_methods),
            "items": items,
        }
        orders.append(order_record)

        # Inject duplicate order (exact duplicate) in 3% of cases
        if random.random() < 0.03:
            orders.append(order_record.copy())

    return orders


def main():
    print("Generating enterprise-scale dirty retail dataset (50,000+ orders)...")
    stores_df = generate_stores(50)
    products_df = generate_products(500)
    customers_df = generate_customers(5000)
    orders = generate_orders_batch(customers_df, stores_df, products_df, 50000)

    stores_path = RAW_DATA_DIR / "stores.csv"
    products_path = RAW_DATA_DIR / "products.csv"
    customers_path = RAW_DATA_DIR / "customers.csv"
    orders_path = RAW_DATA_DIR / "orders.json"

    stores_df.to_csv(stores_path, index=False)
    products_df.to_csv(products_path, index=False)
    customers_df.to_csv(customers_path, index=False)

    with open(orders_path, "w", encoding="utf-8") as f:
        json.dump(orders, f)

    print("Generation complete:")
    print(f"  - Stores:    {len(stores_df):,} records ({stores_path})")
    print(f"  - Products:  {len(products_df):,} records ({products_path})")
    print(f"  - Customers: {len(customers_df):,} records ({customers_path})")
    print(f"  - Orders:    {len(orders):,} records ({orders_path})")


if __name__ == "__main__":
    main()