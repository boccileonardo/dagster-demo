import os
import random
from datetime import datetime, timedelta
import polars as pl
from faker import Faker

fake = Faker()
DATA_DIR = "faker/data"


# --- Uniform date generation ---
def generate_uniform_dates(start_date, end_date, missing_prob=0.05):
    num_days = (end_date - start_date).days + 1
    all_dates = [start_date + timedelta(days=i) for i in range(num_days)]
    # Simulate a few missing dates
    dates = [d for d in all_dates if random.random() > missing_prob]
    return dates


# --- Constants ---
START_DATE = datetime.today() - timedelta(days=90)
END_DATE = datetime.today()
DATES = generate_uniform_dates(START_DATE, END_DATE, missing_prob=0.05)
PRODUCTS = [fake.unique.word() for _ in range(20)]
STORES = [f"Store_{i:03d}" for i in range(1, 6)]


# --- Additional attribute generators ---
def random_currency():
    return random.choice(["USD", "EUR", "GBP", "JPY", "CNY"])


def random_channel():
    return random.choice(
        ["Supermarket", "Convenience", "Online", "Wholesale", "Specialty"]
    )


def random_sector():
    return random.choice(["Beauty", "Health", "Household"])


def random_subcategory():
    return random.choice(
        ["Diapers", "Razors", "Feminine hygiene", "Cosmetics", "Shampoos"]
    )


def random_gtin():
    return str(fake.unique.ean(length=13))


def random_location_number():
    return fake.unique.ean(length=13)


def random_lat():
    return round(random.uniform(-90, 90), 6)


def random_lon():
    return round(random.uniform(-180, 180), 6)


def random_address():
    return fake.address().replace("\n", ", ")


def random_date(start_year=2024, end_year=2025):
    return fake.date_between(
        start_date=f"{start_year}-01-01", end_date=f"{end_year}-06-01"
    )


# --- Enhanced record generators ---
def generate_sales_record(date, store, product, full=False, partial=False):
    base = {
        "date": date.strftime("%Y-%m-%d"),
        "store": store,
        "product": product,
        "sales_qty": random.randint(0, 50),
        "price": round(random.uniform(1.0, 100.0), 2),
    }
    if full:
        base.update(
            {
                "sales_value_usd": round(base["sales_qty"] * base["price"], 2),
                "sales_value_local_currency": round(
                    base["sales_qty"] * base["price"] * random.uniform(0.8, 1.2), 2
                ),
                "return_amount": random.randint(0, 5),
                "return_value_local_currency": round(random.uniform(0, 50), 2),
            }
        )
    elif partial:
        base.update(
            {
                "sales_value_usd": round(base["sales_qty"] * base["price"], 2),
                # No return fields
            }
        )
    # remove price column
    base.pop("price")
    return base


def generate_inventory_record(date, store, product, full=False, partial=False):
    base = {
        "date": date.strftime("%Y-%m-%d"),
        "store": store,
        "product": product,
        "inventory_qty": random.randint(0, 200),
    }
    if full:
        base.update(
            {
                "units_on_order": random.randint(0, 100),
                "value_on_hand_usd": round(
                    base["inventory_qty"] * random.uniform(1.0, 100.0), 2
                ),
                "value_on_hand_local_currency": round(
                    base["inventory_qty"]
                    * random.uniform(0.8, 1.2)
                    * random.uniform(1.0, 100.0),
                    2,
                ),
            }
        )
    elif partial:
        base.update(
            {
                "value_on_hand": round(
                    base["inventory_qty"] * random.uniform(1.0, 100.0), 2
                ),
            }
        )
    return base


# --- Enhanced dimension generators ---
def generate_product(full=False, partial=False):
    name = fake.unique.word()
    base = {"product": name, "category": fake.word()}
    if full:
        base.update(
            {
                "name": fake.catch_phrase(),
                "sector": random_sector(),
                "subcategory": random_subcategory(),
                "launch_date": str(random_date()),
                "GTIN": random_gtin(),
            }
        )
    elif partial:
        base.update(
            {
                "name": fake.catch_phrase(),
                "GTIN": random_gtin(),
            }
        )
    return base


def generate_store(full=False, partial=False):
    name = fake.unique.word()
    base = {"store": name, "city": fake.city()}
    if full:
        base.update(
            {
                "address": random_address(),
                "channel": random_channel(),
                "latitude": random_lat(),
                "longitude": random_lon(),
                "global_location_number": random_location_number(),
            }
        )
    elif partial:
        base.update(
            {
                "address": random_address(),
                "channel": random_channel(),
            }
        )
    return base


# --- Modified data generation functions ---
def one_big_table():
    out_dir = os.path.join(DATA_DIR, "one_big_table")
    os.makedirs(out_dir, exist_ok=True)
    records = [
        {
            **generate_sales_record(date, store, product, full=True),
            **generate_inventory_record(date, store, product, full=True),
        }
        for date in DATES
        for store in STORES
        for product in PRODUCTS
    ]
    df = pl.DataFrame(records)
    df.write_parquet(f"{out_dir}/one_big_table.parquet")


def separate_dim_fact():
    out_dir = os.path.join(DATA_DIR, "separate_dim_fact")
    os.makedirs(out_dir, exist_ok=True)
    df_products = pl.DataFrame([generate_product(partial=True) for _ in PRODUCTS])
    df_stores = pl.DataFrame([generate_store(partial=True) for _ in STORES])
    records = [
        generate_sales_record(date, store, product, partial=True)
        for date in DATES
        for store in STORES
        for product in PRODUCTS
    ]
    df_fact = pl.DataFrame(records)
    df_products.write_parquet(f"{out_dir}/dim_products.parquet")
    df_stores.write_parquet(f"{out_dir}/dim_stores.parquet")
    df_fact.write_parquet(f"{out_dir}/fact_sales.parquet")


def files_per_date():
    out_dir = os.path.join(DATA_DIR, "files_per_date")
    os.makedirs(out_dir, exist_ok=True)
    for date in DATES:
        records = [
            generate_sales_record(date, store, product, full=True)
            for store in STORES
            for product in PRODUCTS
        ]
        df = pl.DataFrame(records)
        df.write_parquet(f"{out_dir}/sales_{date.strftime('%Y%m%d')}.parquet")


def single_file_many_dates():
    out_dir = os.path.join(DATA_DIR, "single_file_many_dates")
    os.makedirs(out_dir, exist_ok=True)
    records = [
        generate_sales_record(date, store, product, partial=True)
        for date in DATES
        for store in STORES
        for product in PRODUCTS
    ]
    df = pl.DataFrame(records)
    df.write_parquet(f"{out_dir}/all_dates.parquet")


def files_per_store():
    out_dir = os.path.join(DATA_DIR, "files_per_store")
    os.makedirs(out_dir, exist_ok=True)
    for store in STORES:
        records = [
            generate_inventory_record(date, store, product, full=True)
            for date in DATES
            for product in PRODUCTS
        ]
        df = pl.DataFrame(records)
        df.write_parquet(f"{out_dir}/sales_{store}.parquet")


def daily_files():
    out_dir = os.path.join(DATA_DIR, "daily_files")
    os.makedirs(out_dir, exist_ok=True)
    for date in DATES:
        records = [
            generate_sales_record(date, store, product, partial=True)
            for store in STORES
            for product in PRODUCTS
        ]
        df = pl.DataFrame(records)
        df.write_parquet(f"{out_dir}/daily_{date.strftime('%Y%m%d')}.parquet")


def weekly_files_single_date():
    out_dir = os.path.join(DATA_DIR, "weekly_files_single_date")
    os.makedirs(out_dir, exist_ok=True)
    for i in range(0, len(DATES), 7):
        week_dates = DATES[i : i + 7]
        week_end = week_dates[0]
        records = [
            generate_sales_record(week_end, store, product, full=True)
            for store in STORES
            for product in PRODUCTS
        ]
        df = pl.DataFrame(records)
        df.write_parquet(
            f"{out_dir}/weekly_single_{week_end.strftime('%Y%m%d')}.parquet"
        )


def weekly_files_all_days():
    out_dir = os.path.join(DATA_DIR, "weekly_files_all_days")
    os.makedirs(out_dir, exist_ok=True)
    for i in range(0, len(DATES), 7):
        week_dates = DATES[i : i + 7]
        records = [
            generate_sales_record(date, store, product, partial=True)
            for date in week_dates
            for store in STORES
            for product in PRODUCTS
        ]
        if records:
            week_end = week_dates[0]
            df = pl.DataFrame(records)
            df.write_parquet(
                f"{out_dir}/weekly_all_{week_end.strftime('%Y%m%d')}.parquet"
            )


if __name__ == "__main__":
    one_big_table()
    separate_dim_fact()
    files_per_date()
    single_file_many_dates()
    files_per_store()
    daily_files()
    weekly_files_single_date()
    weekly_files_all_days()
