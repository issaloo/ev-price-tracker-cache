import json
import os

import pandas as pd
import psycopg2
import redis
from dotenv import load_dotenv

# Using .env, load DB variables
load_dotenv()
DB_HOSTNAME = os.getenv("DB_HOSTNAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_PORT = os.getenv("DB_PORT")
DB_PRICE_TABLE = os.getenv("DB_PRICE_TABLE")
CACHE_HOSTNAME = os.getenv("CACHE_HOSTNAME")
CACHE_PORT = os.getenv("CACHE_PORT")

# Using GCF & SM, access secret through mounting as volume
# secret_location = "/postgres/secret"
# with open(secret_location) as f:
#     secret_payload = f.readlines()[0]
secret_payload = ""

# Establish a connection to the PostgreSQL DB
try:
    connection = psycopg2.connect(
        host=DB_HOSTNAME,
        user=DB_USERNAME,
        password=secret_payload,
        dbname=DB_DATABASE,
        port=DB_PORT,
    )
    connection.autocommit = True
    cursor = connection.cursor()
except Exception as e:
    print(f"Error connecting to the database: {e}")

# Using GCF & SM, access secret through mounting as volume
# secret_location = "/redis/secret"
# with open(secret_location) as f:
#     secret_payload = f.readlines()[0]
secret_payload = ""

# Establish connection Redis
try:
    cache = redis.Redis(host=CACHE_HOSTNAME, port=CACHE_PORT, password=secret_payload, decode_responses=True)
except Exception as e:
    print(f"Error connecting to the cache: {e}")


def function(event, context):
    """Cloud Function entry point function."""

    # TODO: insert main here + read sql


def read_sql_file(query_file_path: str, params: dict | None = None):
    """Read sql from file path.

    Args:
    ----
        query_file_path (str): path to query file
    Returns:
        str: query string
    """
    with open(query_file_path, "r") as f:
        query = f.read()
    if params:
        for key, value in params.items():
            query = query.replace(f"$${key}$$", str(value))
    return query


if __name__ == "__main__":
    count_query = read_sql_file(
        query_file_path="sql/get_current_record_count.sql", params={"DB_PRICE_TABLE": DB_PRICE_TABLE}
    )
    cursor.execute(count_query)
    new_record_count = int(cursor.fetchone()[0])
    curr_record_count = int(cache.get("ev_price_count"))
    # # if new_record_count > curr_record_count:
    if new_record_count == curr_record_count:  # TODO: remove this
        # set ev price count to new count
        cache.set("ev_price_count", new_record_count)

        # get last two prices for each brand model
        calc_query = read_sql_file(
            query_file_path="sql/get_two_most_recent_msrp.sql", params={"DB_PRICE_TABLE": DB_PRICE_TABLE}
        )
        cursor.execute(calc_query)
        new_msrp = cursor.fetchall()
        new_msrp_cols = ["brand_name", "model_name", "msrp", "rank", "car_type", "image_src", "model_url"]
        new_msrp = pd.DataFrame(new_msrp, columns=new_msrp_cols)

        # filter to attributes of most recent data
        mask = new_msrp["rank"] == 1
        attribute_cols = ["brand_name", "model_name", "car_type", "image_src", "model_url"]
        new_msrp = new_msrp.loc[mask, attribute_cols].reset_index(drop=True)

        # pivot to brand model to get previous and current prices
        new_msrp_pivot = (
            pd.pivot_table(data=new_msrp, index=["brand_name", "model_name"], columns=["rank"], values="msrp")
            .reset_index()
            .fillna("none")
        )
        new_msrp_pivot = new_msrp_pivot.rename(columns={1: "current_price", 2: "previous_price"})

        # update column names from snake case to camel case
        new_col = []
        for col in new_msrp_pivot.columns:
            sub_col = col.split("_")
            if len(sub_col) > 1:
                sub_col[1:] = [col.capitalize() for col in sub_col[1:]]
            new_col.append("".join(sub_col))
        new_msrp_pivot.columns = new_col

        # create ev price json
        ev_price_json = []
        for brand_name in new_msrp_pivot["brandName"].unique():
            brand_dict = {"brandName": brand_name}
            mask = new_msrp_pivot["brandName"] == brand_name
            sub_brand = new_msrp_pivot.loc[mask].reset_index(drop=True)
            sub_brand_cols = list(sub_brand.columns)
            sub_brand_cols.remove("brandName")
            sub_brand = sub_brand[sub_brand_cols]
            brand_dict["itemDetails"] = sub_brand.to_dict("records")
            ev_price_json.append(brand_dict)
        ev_price_json = json.dumps(ev_price_json)
        cache.set("ev_price_json", ev_price_json)

        # get last year of data

        # create graph data for each model
        # turn into list of data point dictionaries
        # use Nivo and match data format
        cursor.close()
        connection.close()
