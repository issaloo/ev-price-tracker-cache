import json
import os

import numpy as np
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

CACHE_USERNAME = os.getenv("CACHE_USERNAME")

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

# Establish a connection Redis
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

        # if new brand model, then update current brand model json
        car_query = read_sql_file(
            query_file_path="sql/get_all_brand_model.sql", params={"DB_PRICE_TABLE": DB_PRICE_TABLE}
        )
        cursor.execute(car_query)
        brand_model_list = cursor.fetchall()
        brand_model_list = list(map(list, brand_model_list))
        new_brand_model_json = json.dumps(brand_model_list)
        curr_brand_model_json = json.loads(cache.get("brand_model_json"))
        if new_brand_model_json != curr_brand_model_json:
            curr_brand_model_json = cache.set("brand_model_json", new_brand_model_json)

        # calculate last price change
        calc_query = read_sql_file(
            query_file_path="sql/get_two_most_recent_msrp.sql", params={"DB_PRICE_TABLE": DB_PRICE_TABLE}
        )
        cursor.execute(calc_query)
        new_msrp = cursor.fetchall()
        new_msrp = pd.DataFrame(new_msrp, columns=["brand_name", "model_name", "msrp", "rank"])
        new_msrp_pivot = pd.pivot_table(
            data=new_msrp, index=["brand_name", "model_name"], columns=["rank"], values="msrp"
        ).reset_index()
        new_msrp_pivot["msrp_change"] = new_msrp_pivot[1] - new_msrp_pivot[2]
        new_msrp_pivot["msrp_change_pct"] = np.around(
            ((new_msrp_pivot["msrp_change"] / new_msrp_pivot[2]) * 100), decimals=2
        )
        new_msrp_pivot = new_msrp_pivot[["brand_name", "model_name", "msrp_change", "msrp_change_pct"]]
        print(new_msrp_pivot)
        # TODO: set compound key in redis... maybe look into how to get brand_name.model_name.msrp_change as key
