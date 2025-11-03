import os
import shutil
import random
from datetime import datetime, timedelta
import polars as pl
from faker import Faker

fake = Faker()
DATA_DIR = "faker/data"


def clean_data_dir():
    shutil.rmtree(DATA_DIR)


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


def random_package_material():
    return random.choice(["PET", "HDPE", "Cardboard", "Glass", "Aluminum", "PLA"])


def random_brand_tier():
    return random.choice(["Value", "Mainstream", "Premium", "Ultra-Premium"])


def random_shelf_life_days():
    return random.choice([30, 60, 90, 180, 365, 730])


def random_num_components():
    # For bundled products (e.g., multi-pack razors + blades)
    return random.randint(1, 12)


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


def random_format_subtype():
    return random.choice(
        [
            "Urban Compact",
            "Suburban Large",
            "High Street",
            "Mall Anchor",
            "Outlet",
            "Airport",
        ]
    )


def random_lease_type():
    return random.choice(["Owned", "Leased", "Franchise", "Joint-Venture"])


def random_square_footage():
    return random.randint(800, 120000)


def random_parking_spaces():
    return random.randint(0, 800)


def random_manager_name():
    return fake.name()


def random_store_opening_date():
    return fake.date_between(
        start_date=datetime(2000, 1, 1), end_date=datetime(2025, 1, 1)
    )


def random_micro_fulfillment_enabled():
    return random.choice(["Y", "N"])


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
                # uncommon retailer metrics
                "units_shipped_to_store": random.randint(0, 60),
                "promo_flag": random.choice(["Y", "N"]),
                "markdown_pct": round(
                    random.uniform(0, 0.6), 2
                ),  # fraction of price discounted
                "waste_qty": random.randint(0, 3),  # damaged or expired units
                "on_display_qty": random.randint(0, 20),  # units on special display
                "ominchannel_order_qty": random.randint(
                    0, 10
                ),  # online orders fulfilled by store (deliberate uncommon spelling)
                "forecast_sales_qty": max(0, int(random.gauss(base["sales_qty"], 5))),
            }
        )
    else:
        base.update(
            {
                "sales_value_usd": round(base["sales_qty"] * base["price"], 2),
                # No return fields
                # lighter uncommon metrics subset
                "promo_flag": random.choice(["Y", "N"]),
                "markdown_pct": round(random.uniform(0, 0.5), 2),
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
                # uncommon inventory metrics
                "safety_stock_qty": random.randint(0, 50),
                "cycle_count_variance_qty": random.randint(-5, 5),
                "inventory_accuracy_pct": round(random.uniform(0.85, 1.0), 3),
                "days_of_supply": round(random.uniform(0, 60), 1),
                "shrinkage_qty": random.randint(0, 4),  # theft/damage loss
                "reserved_qty_for_online": random.randint(0, 30),
                "backroom_qty": random.randint(0, 120),
            }
        )
    else:
        base.update(
            {
                "value_on_hand": round(
                    base["inventory_qty"] * random.uniform(1.0, 100.0), 2
                ),
                # partial subset of uncommon metrics
                "safety_stock_qty": random.randint(0, 40),
                "inventory_accuracy_pct": round(random.uniform(0.8, 1.0), 3),
            }
        )
    return base


# --- Dimension generators ---
def generate_product(name, partial=False):
    base = {
        # 'id' will be assigned later with with_row_index
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
                # uncommon product dimension attributes
                "logo_present_on_pack": random.choice(["Y", "N"]),
                "package_material": random_package_material(),
                "shelf_life_days": random_shelf_life_days(),
                "is_bundle": random.choice(["Y", "N"]),
                "brand_marketing_tier": random_brand_tier(),
                "num_components": random_num_components(),
            }
        )
    else:
        base.update(
            {
                "GTIN": random_gtin(),
                # partial subset of uncommon attributes
                "logo_present_on_pack": random.choice(["Y", "N"]),
                "package_material": random_package_material(),
            }
        )
    return base


def generate_store(name, partial=False):
    base = {
        # 'id' will be assigned later with with_row_index
        "store": name,
        "city": fake.city(),
    }
    if not partial:
        base.update(
            {
                "address": random_address(),
                "channel": random_channel(),
                "latitude": random_lat(),
                "longitude": random_lon(),
                "global_location_number": random_location_number(),
                # uncommon store dimension attributes
                "store_manager_name": random_manager_name(),
                "store_opening_date": str(random_store_opening_date()),
                "format_subtype": random_format_subtype(),
                "store_square_footage": random_square_footage(),
                "lease_type": random_lease_type(),
                "parking_spaces": random_parking_spaces(),
                "micro_fulfillment_enabled": random_micro_fulfillment_enabled(),
            }
        )
    else:
        base.update(
            {
                "address": random_address(),
                "channel": random_channel(),
                # partial subset
                "format_subtype": random_format_subtype(),
                "lease_type": random_lease_type(),
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
    return None, None


def separate_dim_fact(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "separate_dim_fact")
    os.makedirs(out_dir, exist_ok=True)
    df_products = pl.DataFrame(
        [generate_product(name=product, partial=False) for product in PRODUCTS]
    ).with_row_index("id")
    df_stores = pl.DataFrame(
        [generate_store(name=store, partial=False) for store in STORES]
    ).with_row_index("id")
    products_in_dim = df_products["id"].to_list()
    stores_in_dim = df_stores["id"].to_list()
    product_name_map = dict(zip(df_products["id"], df_products["product"]))
    store_name_map = dict(zip(df_stores["id"], df_stores["store"]))
    records = [
        generate_sales_record(date, store_id, product_id, partial=True)
        for date in DATES
        for store_id in stores_in_dim
        for product_id in products_in_dim
    ]
    # Add product/store names to fact table for clarity
    for rec in records:
        rec["product_name"] = product_name_map[rec["product"]]
        rec["store_name"] = store_name_map[rec["store"]]
    df_fact = pl.DataFrame(records)

    write_df(df_products, f"{out_dir}/dim_products.{file_format}", file_format)
    write_df(df_stores, f"{out_dir}/dim_stores.{file_format}", file_format)
    write_df(df_fact, f"{out_dir}/fact_sales.{file_format}", file_format)
    return df_products, df_stores


def single_file_many_dates(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "single_file_many_dates")
    os.makedirs(out_dir, exist_ok=True)
    # Load dimension tables
    df_products = pl.DataFrame(
        [generate_product(name=product, partial=False) for product in PRODUCTS]
    ).with_row_index("product_id")
    df_stores = pl.DataFrame(
        [generate_store(name=store, partial=False) for store in STORES]
    ).with_row_index("store_id")
    product_name_to_row = {row["product"]: row for row in df_products.to_dicts()}
    store_name_to_row = {row["store"]: row for row in df_stores.to_dicts()}
    records = []
    barebones_allowed_keys = {
        "date",
        "store",
        "product",
        "sales_qty",
        "sales_value_usd",
        "sales_value_local_currency",
        "return_amount",
        "return_value_local_currency",
        # product dimension original attributes
        "category",
        "sector",
        "launch_date",
        "GTIN",
        "description",
        # store dimension original attributes
        "city",
        "address",
        "channel",
        "latitude",
        "longitude",
        "global_location_number",
    }
    for date in DATES:
        for store in STORES:
            for product in PRODUCTS:
                rec = generate_sales_record(date, store, product, partial=False)
                # Merge all product/store attributes into the record
                rec.update(product_name_to_row[product])
                rec.update(store_name_to_row[store])
                # Prune any newly added uncommon columns (fact + dimension)
                pruned = {k: v for k, v in rec.items() if k in barebones_allowed_keys}
                records.append(pruned)
    df = pl.DataFrame(records)
    write_df(df, f"{out_dir}/all_dates.{file_format}", file_format)
    return df_products, df_stores


def files_per_store(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "files_per_store")
    os.makedirs(out_dir, exist_ok=True)
    df_products = pl.DataFrame(
        [generate_product(name=product, partial=False) for product in PRODUCTS]
    ).with_row_index("product_id")
    df_stores = pl.DataFrame(
        [generate_store(name=store, partial=False) for store in STORES]
    ).with_row_index("store_id")
    product_name_to_row = {row["product"]: row for row in df_products.to_dicts()}
    store_name_to_row = {row["store"]: row for row in df_stores.to_dicts()}
    for store in STORES:
        records = []
        for date in DATES:
            for product in PRODUCTS:
                rec = generate_inventory_record(date, store, product, partial=False)
                # Merge all product/store attributes into the record
                rec.update(product_name_to_row[product])
                rec.update(store_name_to_row[store])
                records.append(rec)
        df = pl.DataFrame(records)
        write_df(df, f"{out_dir}/sales_{store}.{file_format}", file_format)
    return df_products, df_stores


def daily_files(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "daily_files")
    os.makedirs(out_dir, exist_ok=True)
    df_products = pl.DataFrame(
        [generate_product(name=product, partial=False) for product in PRODUCTS]
    ).with_row_index("product_id")
    df_stores = pl.DataFrame(
        [generate_store(name=store, partial=False) for store in STORES]
    ).with_row_index("store_id")
    product_name_to_id = dict(zip(df_products["product"], df_products["product_id"]))
    store_name_to_id = dict(zip(df_stores["store"], df_stores["store_id"]))
    for date in DATES:
        records = []
        for store in STORES:
            for product in PRODUCTS:
                rec = generate_sales_record(date, store, product, partial=True)
                rec["product_id"] = product_name_to_id[product]
                rec["store_id"] = store_name_to_id[store]
                # Remove uncommon partial sales columns for barebones daily feed
                for drop_key in ["promo_flag", "markdown_pct"]:
                    if drop_key in rec:
                        del rec[drop_key]
                records.append(rec)
        df = pl.DataFrame(records)
        write_df(
            df, f"{out_dir}/daily_{date.strftime('%Y%m%d')}.{file_format}", file_format
        )
    return df_products, df_stores


def weekly_files_single_date(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "weekly_files_single_date")
    os.makedirs(out_dir, exist_ok=True)
    df_products = pl.DataFrame(
        [generate_product(name=product, partial=False) for product in PRODUCTS]
    ).with_row_index("product_id")
    df_stores = pl.DataFrame(
        [generate_store(name=store, partial=False) for store in STORES]
    ).with_row_index("store_id")
    product_name_to_row = {row["product"]: row for row in df_products.to_dicts()}
    store_name_to_row = {row["store"]: row for row in df_stores.to_dicts()}
    for i in range(0, len(DATES), 7):
        week_dates = DATES[i : i + 7]
        week_end = week_dates[0]
        records = []
        for store in STORES:
            for product in PRODUCTS:
                rec = generate_sales_record(week_end, store, product, partial=False)
                # Merge all product/store attributes into the record
                rec.update(product_name_to_row[product])
                rec.update(store_name_to_row[store])
                records.append(rec)
        df = pl.DataFrame(records)
        write_df(
            df,
            f"{out_dir}/weekly_single_{week_end.strftime('%Y%m%d')}.{file_format}",
            file_format,
        )
    return df_products, df_stores


def weekly_files_all_days(file_format="parquet"):
    out_dir = os.path.join(DATA_DIR, "weekly_files_all_days")
    os.makedirs(out_dir, exist_ok=True)
    df_products = pl.DataFrame(
        [generate_product(name=product, partial=False) for product in PRODUCTS]
    ).with_row_index("product_id")
    df_stores = pl.DataFrame(
        [generate_store(name=store, partial=False) for store in STORES]
    ).with_row_index("store_id")
    product_name_to_id = dict(zip(df_products["product"], df_products["product_id"]))
    store_name_to_id = dict(zip(df_stores["store"], df_stores["store_id"]))
    for i in range(0, len(DATES), 7):
        week_dates = DATES[i : i + 7]
        records = []
        for date in week_dates:
            for store in STORES:
                for product in PRODUCTS:
                    rec = generate_sales_record(date, store, product, partial=True)
                    rec["product_id"] = product_name_to_id[product]
                    rec["store_id"] = store_name_to_id[store]
                    records.append(rec)
        if records:
            week_end = week_dates[0]
            df = pl.DataFrame(records)
            write_df(
                df,
                f"{out_dir}/weekly_all_{week_end.strftime('%Y%m%d')}.{file_format}",
                file_format,
            )
    return df_products, df_stores


def fake_corporate_product_master_data(all_products_dfs):
    all_products_dfs = [
        df.rename({"product_id": "id"}) if "product_id" in df.columns else df
        for df in all_products_dfs
    ]
    all_products = (
        pl.concat(all_products_dfs)
        .unique(subset=["GTIN"])
        .select("GTIN", "product", "category", "sector", "description")
    )
    sampled_products = all_products.sample(fraction=0.3)
    sampled_products = sampled_products.select(
        pl.col("GTIN").alias("item_gtin").cast(pl.Int64),
        pl.col("product").alias("prod_name"),
        pl.col("sector"),
        pl.col("category"),
        pl.col("description").alias("item_description"),
    )
    sampled_products.write_parquet("faker/data/corporate_product_master_data.parquet")


def fake_corporate_site_master_data(all_stores_dfs):
    all_stores_dfs = [
        df.rename({"store_id": "id"}) if "store_id" in df.columns else df
        for df in all_stores_dfs
    ]
    all_sites = (
        pl.concat(all_stores_dfs)
        .unique(subset=["global_location_number"])
        .select(
            "global_location_number",
            "store",
            "address",
            "channel",
            "latitude",
            "longitude",
        )
    )
    sampled_sites = all_sites.sample(fraction=0.3)
    sampled_sites = sampled_sites.select(
        pl.col("global_location_number").cast(pl.Int64),
        pl.col("store").alias("site_name"),
        pl.col("address"),
        pl.col("channel"),
        pl.col("latitude"),
        pl.col("longitude"),
    )
    sampled_sites.write_parquet("faker/data/corporate_site_master_data.parquet")


if __name__ == "__main__":
    clean_data_dir()
    all_products_dfs = []
    all_stores_dfs = []

    data_gens = [
        one_big_table("json"),
        separate_dim_fact("csv"),
        single_file_many_dates(),
        files_per_store("txt"),
        daily_files(),
        weekly_files_single_date(),
        weekly_files_all_days(),
    ]

    for prod_df, store_df in data_gens:
        if prod_df is not None:
            all_products_dfs.append(prod_df)
        if store_df is not None:
            all_stores_dfs.append(store_df)

    fake_corporate_product_master_data(all_products_dfs)
    fake_corporate_site_master_data(all_stores_dfs)
