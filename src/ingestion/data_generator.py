"""
src/ingestion/data_generator.py

Generates synthetic CSV files for the domains that don't come from the
Online Retail II dataset: suppliers, stores, inventory snapshots,
promotions, and shipping. Also derives customer/product master files
from Online Retail II with added synthetic attributes.

Run this ONCE (or whenever you want a fresh landing zone) BEFORE running
the Bronze pipeline for the first time - Bronze reads from data/landing/,
and nothing exists there until this script (or a real source system) puts
files there.

Referential integrity is enforced deliberately: every generated store_id/
product_id/customer_id used in supporting tables is drawn from the SAME
pool used elsewhere, so foreign keys always resolve - see Day 2 of the
roadmap for why this matters (orphaned FKs are a common beginner mistake
in synthetic data generation).
"""

from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timedelta

from faker import Faker

from src.common.config_loader import load_app_config

fake = Faker()
Faker.seed(42)
random.seed(42)

# Resolved from config/<env>_config.yaml's paths.landing_zone - local
# "data/landing" by default, or the Unity Catalog Volume path when
# ENV=databricks. This is the fix for the earlier hardcoded LANDING_DIR,
# which silently wrote to the wrong place once paths diverged per
# environment.
LANDING_DIR = load_app_config()["paths"]["landing_zone"]

COUNTRIES = ["United Kingdom", "France", "Germany", "USA", "Australia"]
REGIONS = {
    "United Kingdom": "EMEA", "France": "EMEA", "Germany": "EMEA",
    "USA": "AMERICAS", "Australia": "APAC",
}

NUM_STORES = 20
NUM_SUPPLIERS = 15
NUM_PRODUCTS = 200          # kept small here for a fast demo; Online Retail II provides real products at scale
NUM_CUSTOMERS = 500         # same - real run should source these from Online Retail II instead
SNAPSHOT_DAYS = 30          # inventory snapshot days to generate (30 for demo; 365 at full portfolio scale)


def _ensure_dir(table_name: str) -> str:
    path = os.path.join(LANDING_DIR, table_name)
    os.makedirs(path, exist_ok=True)
    return path


def _write_csv(table_name: str, filename: str, fieldnames: list[str], rows: list[dict]) -> None:
    path = _ensure_dir(table_name)
    filepath = os.path.join(path, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {filepath}")


def generate_stores() -> list[str]:
    """Returns the list of generated store_ids, so other generators can
    reference real store IDs (referential integrity)."""
    store_ids = []
    rows = []
    for i in range(1, NUM_STORES + 1):
        store_id = f"ST{i:03d}"
        country = random.choice(COUNTRIES)
        store_ids.append(store_id)
        rows.append({
            "store_id": store_id,
            "region": REGIONS[country],
            "country": country,
            "store_name": f"{fake.city()} Store",
        })
    _write_csv("store", "stores.csv", ["store_id", "region", "country", "store_name"], rows)
    return store_ids


def generate_suppliers() -> list[str]:
    supplier_ids = []
    rows = []
    for i in range(1, NUM_SUPPLIERS + 1):
        supplier_id = f"SUP{i:03d}"
        supplier_ids.append(supplier_id)
        rows.append({
            "supplier_id": supplier_id,
            "supplier_name": fake.company(),
        })
    _write_csv("supplier", "suppliers.csv", ["supplier_id", "supplier_name"], rows)
    return supplier_ids


def generate_products(supplier_ids: list[str]) -> list[str]:
    """
    In a full run, product_id/stock_code should come from the Online
    Retail II dataset itself (see Day 1 of the roadmap). This generator
    produces STANDALONE demo products so the pipeline is runnable
    end-to-end without first downloading the external dataset - swap
    this out for real stock_codes once you've pulled Online Retail II.
    """
    stock_codes = []
    rows = []
    for i in range(1, NUM_PRODUCTS + 1):
        stock_code = f"A{100 + i}"
        stock_codes.append(stock_code)
        rows.append({
            "stock_code": stock_code,
            "description": fake.catch_phrase(),
            "unit_price": round(random.uniform(2.0, 150.0), 2),
            "supplier_id": random.choice(supplier_ids),
        })
    _write_csv("product", "products.csv", ["stock_code", "description", "unit_price", "supplier_id"], rows)
    return stock_codes


def generate_customers() -> list[str]:
    """
    Same caveat as generate_products: real customer_ids should come
    from Online Retail II. This produces standalone demo customers so
    the pipeline runs end-to-end immediately.
    """
    customer_ids = []
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customer_id = f"C{1000 + i}"
        customer_ids.append(customer_id)
        rows.append({
            "customer_id": customer_id,
            "country": random.choice(COUNTRIES),
            "segment": random.choice(["Retail", "Wholesale", "Online-Only"]),
        })
    _write_csv("customer", "customers.csv", ["customer_id", "country", "segment"], rows)
    return customer_ids


def generate_sales(customer_ids: list[str], stock_codes: list[str], store_ids: list[str], num_transactions: int = 2000) -> list[str]:
    invoice_ids = []
    rows = []
    base_date = datetime(2026, 1, 1)
    for i in range(1, num_transactions + 1):
        invoice_id = f"INV{100000 + i}"
        invoice_ids.append(invoice_id)
        sale_date = base_date + timedelta(days=random.randint(0, 200))
        rows.append({
            "invoice_id": invoice_id,
            "stock_code": random.choice(stock_codes),
            "quantity": random.randint(1, 20),
            "unit_price": round(random.uniform(2.0, 150.0), 2),
            "customer_id": random.choice(customer_ids),
            "store_id": random.choice(store_ids),
            "country": random.choice(COUNTRIES),
            "invoice_date": sale_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "sale_date": sale_date.strftime("%Y-%m-%d"),
        })
    _write_csv(
        "sales", "sales_20260727.csv",
        ["invoice_id", "stock_code", "quantity", "unit_price", "customer_id", "store_id", "country", "invoice_date", "sale_date"],
        rows,
    )
    return invoice_ids


def generate_inventory_snapshots(store_ids: list[str], stock_codes: list[str]) -> None:
    """
    Periodic snapshot fact - one row per (store, product, day). This is
    deliberately the largest generated table (see Phase 4's data volume
    rationale) - reduce SNAPSHOT_DAYS or sample store/product pairs if
    running on a resource-constrained machine.
    """
    rows = []
    base_date = datetime(2026, 7, 1)
    # Sample a subset of store-product pairs per day rather than the full
    # cross-product, to keep the demo runnable on Community Edition -
    # at full portfolio scale you'd generate the complete cross-join.
    for day_offset in range(SNAPSHOT_DAYS):
        snapshot_date = (base_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        for store_id in store_ids:
            for stock_code in random.sample(stock_codes, k=min(30, len(stock_codes))):
                rows.append({
                    "store_id": store_id,
                    "stock_code": stock_code,
                    "snapshot_date": snapshot_date,
                    "stock_on_hand": random.randint(0, 500),
                })
    _write_csv(
        "inventory", "inventory_snapshots.csv",
        ["store_id", "stock_code", "snapshot_date", "stock_on_hand"],
        rows,
    )


def generate_promotions(stock_codes: list[str]) -> None:
    rows = []
    base_date = datetime(2026, 1, 1)
    for i in range(1, 31):
        start = base_date + timedelta(days=random.randint(0, 150))
        end = start + timedelta(days=random.randint(3, 21))
        rows.append({
            "promo_id": f"PROMO{i:03d}",
            "stock_code": random.choice(stock_codes),
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "discount_pct": round(random.uniform(5.0, 40.0), 1),
        })
    _write_csv("promotions", "promotions.csv", ["promo_id", "stock_code", "start_date", "end_date", "discount_pct"], rows)


def generate_online_orders(customer_ids: list[str], stock_codes: list[str], num_orders: int = 500) -> list[str]:
    order_ids = []
    rows = []
    base_date = datetime(2026, 1, 1)
    statuses = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
    for i in range(1, num_orders + 1):
        order_id = f"ORD{5000 + i}"
        order_ids.append(order_id)
        order_date = base_date + timedelta(days=random.randint(0, 200))
        rows.append({
            "order_id": order_id,
            "customer_id": random.choice(customer_ids),
            "stock_code": random.choice(stock_codes),
            "order_status": random.choice(statuses),
            "order_date": order_date.strftime("%Y-%m-%d"),
        })
    _write_csv("online_orders", "online_orders.csv", ["order_id", "customer_id", "stock_code", "order_status", "order_date"], rows)
    return order_ids


def generate_shipping(order_ids: list[str]) -> None:
    rows = []
    carriers = ["DHL", "FedEx", "UPS", "Royal Mail"]
    base_date = datetime(2026, 1, 2)
    for i, order_id in enumerate(order_ids, start=1):
        if random.random() < 0.8:  # not every order has shipped yet - realistic
            ship_date = base_date + timedelta(days=random.randint(0, 200))
            rows.append({
                "shipment_id": f"SHIP{i:05d}",
                "order_id": order_id,
                "carrier": random.choice(carriers),
                "ship_date": ship_date.strftime("%Y-%m-%d"),
            })
    _write_csv("shipping", "shipping.csv", ["shipment_id", "order_id", "carrier", "ship_date"], rows)


def generate_returns(invoice_ids: list[str], stock_codes: list[str], num_returns: int = 100) -> None:
    rows = []
    for i in range(1, num_returns + 1):
        rows.append({
            "return_invoice_id": f"RET{i:05d}",
            "original_invoice_id": random.choice(invoice_ids),
            "stock_code": random.choice(stock_codes),
            "quantity": random.randint(1, 5),
        })
    _write_csv("returns", "returns.csv", ["return_invoice_id", "original_invoice_id", "stock_code", "quantity"], rows)


def main():
    print("Generating synthetic landing zone data...")
    print(f"Output directory: {os.path.abspath(LANDING_DIR)}\n")

    store_ids = generate_stores()
    supplier_ids = generate_suppliers()
    stock_codes = generate_products(supplier_ids)
    customer_ids = generate_customers()
    invoice_ids = generate_sales(customer_ids, stock_codes, store_ids)
    generate_inventory_snapshots(store_ids, stock_codes)
    generate_promotions(stock_codes)
    order_ids = generate_online_orders(customer_ids, stock_codes)
    generate_shipping(order_ids)
    generate_returns(invoice_ids, stock_codes)

    print("\nDone. All FKs (store_id, stock_code, customer_id, order_id, invoice_id) "
          "are drawn from the same generated pools, so every foreign key resolves.")


if __name__ == "__main__":
    main()
