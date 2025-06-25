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
PRODUCTS = [fake.name() for _ in range(200)]
STORES = [f"Store_{i:03d}" for i in range(1, 50)]


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
        start_date=datetime(start_year, 1, 1), end_date=datetime(end_year, 6, 1)
    )


# --- Record generators ---
def generate_sales_record(date, store, product, partial=False):
    base = {
        "date": date.strftime("%Y-%m-%d"),
        "store": store,
        "product": product,
        "sales_qty": random.randint(0, 50),
        "price": round(random.uniform(1.0, 100.0), 2),
    }
    if not partial:
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
    else:
        base.update(
            {
                "sales_value_usd": round(base["sales_qty"] * base["price"], 2),
                # No return fields
            }
        )
    # remove price column
    base.pop("price")
    return base


def generate_inventory_record(date, store, product, partial=False):
    base = {
        "date": date.strftime("%Y-%m-%d"),
        "store": store,
        "product": product,
        "inventory_qty": random.randint(0, 200),
    }
    if not partial:
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
    else:
        base.update(
            {
                "value_on_hand": round(
                    base["inventory_qty"] * random.uniform(1.0, 100.0), 2
                ),
            }
        )
    return base


# --- Dimension generators ---
def generate_product(name, partial=False):
    base = {
        "id": random.randint(1000, 9999),
        "product": name,
        "category": random_subcategory(),
    }
    if not partial:
        base.update(
            {
                "sector": random_sector(),
                "launch_date": str(random_date()),
                "GTIN": random_gtin(),
                "description": fake.catch_phrase(),
            }
        )
    else:
        base.update(
            {
                "GTIN": random_gtin(),
            }
        )
    return base


def generate_store(name, partial=False):
    base = {"id": random.randint(1000, 9999), "store": name, "city": fake.city()}
    if not partial:
        base.update(
            {
                "address": random_address(),
                "channel": random_channel(),
                "latitude": random_lat(),
                "longitude": random_lon(),
                "global_location_number": random_location_number(),
            }
        )
    else:
        base.update(
            {
                "address": random_address(),
                "channel": random_channel(),
            }
        )
    return base


# --- Data generation functions ---
def write_df(df: pl.DataFrame, path, file_format):
    """Write a Polars DataFrame to the specified format."""
    if file_format == "parquet":
        df.write_parquet(path)
    elif file_format == "csv":
        df.write_csv(path)
    elif file_format == "json":
        df.write_json(path)
    elif file_format == "txt":
        # Write as CSV but with .txt extension and pipe separator.
        df.write_csv(path, separator="|")
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def one_big_table(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "one_big_table")
    os.makedirs(out_dir, exist_ok=True)
    records = [
        {
            **generate_sales_record(date, store, product, partial=True),
            **generate_inventory_record(date, store, product, partial=True),
        }
        for date in DATES
        for store in STORES
        for product in PRODUCTS
    ]
    df = pl.DataFrame(records)

    write_df(df, f"{out_dir}/one_big_table.{file_format}", file_format)


def separate_dim_fact(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "separate_dim_fact")
    os.makedirs(out_dir, exist_ok=True)
    df_products = pl.DataFrame(
        [generate_product(name=product, partial=False) for product in PRODUCTS]
    )
    df_stores = pl.DataFrame(
        [generate_store(name=store, partial=False) for store in STORES]
    )
    products_in_dim = df_products.select("id").unique().to_series().to_list()
    stores_in_dim = df_products.select("id").unique().to_series().to_list()
    records = [
        generate_sales_record(date, store, product, partial=True)
        for date in DATES
        for store in stores_in_dim
        for product in products_in_dim
    ]
    df_fact = pl.DataFrame(records)

    write_df(df_products, f"{out_dir}/dim_products.{file_format}", file_format)
    write_df(df_stores, f"{out_dir}/dim_stores.{file_format}", file_format)
    write_df(df_fact, f"{out_dir}/fact_sales.{file_format}", file_format)


def single_file_many_dates(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "single_file_many_dates")
    os.makedirs(out_dir, exist_ok=True)
    records = [
        generate_sales_record(date, store, product, partial=False)
        for date in DATES
        for store in STORES
        for product in PRODUCTS
    ]
    df = pl.DataFrame(records)

    write_df(df, f"{out_dir}/all_dates.{file_format}", file_format)


def files_per_store(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "files_per_store")
    os.makedirs(out_dir, exist_ok=True)

    for store in STORES:
        records = [
            generate_inventory_record(date, store, product, partial=False)
            for date in DATES
            for product in PRODUCTS
        ]
        df = pl.DataFrame(records)
        write_df(df, f"{out_dir}/sales_{store}.{file_format}", file_format)


def daily_files(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "daily_files")
    os.makedirs(out_dir, exist_ok=True)

    for date in DATES:
        records = [
            generate_sales_record(date, store, product, partial=True)
            for store in STORES
            for product in PRODUCTS
        ]
        df = pl.DataFrame(records)
        write_df(
            df, f"{out_dir}/daily_{date.strftime('%Y%m%d')}.{file_format}", file_format
        )


def weekly_files_single_date(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "weekly_files_single_date")
    os.makedirs(out_dir, exist_ok=True)

    for i in range(0, len(DATES), 7):
        week_dates = DATES[i : i + 7]
        week_end = week_dates[0]
        records = [
            generate_sales_record(week_end, store, product, partial=False)
            for store in STORES
            for product in PRODUCTS
        ]
        df = pl.DataFrame(records)
        write_df(
            df,
            f"{out_dir}/weekly_single_{week_end.strftime('%Y%m%d')}.{file_format}",
            file_format,
        )


def weekly_files_all_days(file_format="parquet"):
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
            write_df(
                df,
                f"{out_dir}/weekly_all_{week_end.strftime('%Y%m%d')}.{file_format}",
                file_format,
            )


def fake_corporate_product_master_data():
    all_products = (
        pl.scan_csv("faker/data/separate_dim_fact/dim_products.csv")
        .select("GTIN")
        .unique()
    )
    sampled_products = all_products.collect(engine="streaming").sample(fraction=0.3)
    sampled_products = sampled_products.select(
        pl.col("GTIN").alias("item_gtin"),
        pl.col("GTIN").alias("prod_name"),
        pl.lit(random_sector()).alias("sector"),
        pl.lit(random_sector()).alias("category"),
        pl.lit(random_subcategory()).alias("subcategory"),
        pl.lit(random_subcategory()).alias("item_description"),
    )
    sampled_products.write_parquet("faker/data/corporate_product_master_data.parquet")


def fake_corporate_site_master_data():
    all_sites = (
        pl.scan_csv("faker/data/separate_dim_fact/dim_stores.csv")
        .select("global_location_number")
        .unique()
    )
    sampled_sites = all_sites.collect(engine="streaming").sample(fraction=0.3)
    sampled_sites = sampled_sites.select(
        pl.col("global_location_number"),
        pl.col("global_location_number").alias("site_name"),
        pl.lit(random_address()).alias("address"),
        pl.lit(random_channel()).alias("channel"),
        pl.lit(random_lat()).alias("latitude"),
        pl.lit(random_lon()).alias("longitude"),
    )
    sampled_sites.write_parquet("faker/data/corporate_site_master_data.parquet")


if __name__ == "__main__":
    one_big_table("json")
    separate_dim_fact("csv")
    single_file_many_dates()
    files_per_store("txt")
    daily_files()
    weekly_files_single_date()
    weekly_files_all_days()
    fake_corporate_product_master_data()
    fake_corporate_site_master_data()
