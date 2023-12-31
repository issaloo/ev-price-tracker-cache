import json
import os
from datetime import date

import pandas as pd
import psycopg2
import redis
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv


def run_ev_price_cache(event, context):
    """Cloud Function entry point function."""

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

    #########################
    # Environment Variables #
    #########################
    load_dotenv()
    DB_HOSTNAME = os.getenv("DB_HOSTNAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_PORT = os.getenv("DB_PORT")
    DB_PRICE_TABLE = os.getenv("DB_PRICE_TABLE")
    CACHE_HOSTNAME = os.getenv("CACHE_HOSTNAME")
    CACHE_PORT = os.getenv("CACHE_PORT")
    CACHE_VERSION = os.getenv("CACHE_VERSION")
    KEY_PREFIX = f":{CACHE_VERSION}:"

    #######################
    # PostgreSQL DataBase #
    #######################

    # Using GCF & SM, access secret through mounting as volume
    secret_location = "/postgres/secret"
    with open(secret_location) as f:
        secret_payload = f.readlines()[0]

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
    secret_location = "/redis/secret"
    with open(secret_location) as f:
        secret_payload = f.readlines()[0]

    # Establish connection Redis
    try:
        cache = redis.Redis(host=CACHE_HOSTNAME, port=CACHE_PORT, password=secret_payload, decode_responses=True)
    except Exception as e:
        print(f"Error connecting to the cache: {e}")

    #########################
    # EV Price Landing JSON #
    #########################

    # get last two prices for each brand model
    calc_query = read_sql_file(
        query_file_path="sql/get_two_most_recent_msrp.sql", params={"DB_PRICE_TABLE": DB_PRICE_TABLE}
    )
    cursor.execute(calc_query)
    new_msrp = cursor.fetchall()
    new_msrp = pd.DataFrame(new_msrp, columns=["brand_name", "model_name", "msrp", "image_src", "model_url", "rank"])

    # filter to attributes and pivot to brand model to get previous and current prices
    mask = new_msrp["rank"] == 1
    attribute_cols = ["brand_name", "model_name", "image_src", "model_url"]
    attr_df = new_msrp.loc[mask, attribute_cols].reset_index(drop=True)
    new_msrp_pivot = (
        pd.pivot_table(data=new_msrp, index=["brand_name", "model_name"], columns="rank", values="msrp", aggfunc="sum")
        .reset_index()
        .fillna("none")
    )
    new_msrp_pivot = new_msrp_pivot.rename(columns={1: "current_price", 2: "previous_price"})
    attr_df = attr_df.merge(new_msrp_pivot, on=["brand_name", "model_name"], how="left").sort_values(
        by=["brand_name", "current_price"], ascending=[False, False]
    )

    # update column names from snake case to camel case
    new_col = []
    for col in attr_df.columns:
        sub_col = col.split("_")
        if len(sub_col) > 1:
            sub_col[1:] = [col.capitalize() for col in sub_col[1:]]
        new_col.append("".join(sub_col))
    attr_df.columns = new_col

    # create ev price json and store in redis
    ev_price_json = []
    for brand_name in attr_df["brandName"].unique():
        brand_dict = {"brandName": brand_name}
        mask = attr_df["brandName"] == brand_name
        sub_attr = attr_df.loc[mask].reset_index(drop=True)
        sub_attr_cols = list(sub_attr.columns)
        sub_attr_cols.remove("brandName")
        sub_attr = sub_attr[sub_attr_cols]
        brand_dict["itemDetails"] = sub_attr.to_dict("records")
        ev_price_json.append(brand_dict)
    ev_price_json = json.dumps(ev_price_json)
    cache.set(f"{KEY_PREFIX}ev_price_json", ev_price_json)

    ##################################
    # EV Graph JSON for Each Vehicle #
    ##################################

    # for every brand model, update graph data json
    brand_model_list = new_msrp[["brand_name", "model_name"]].drop_duplicates().to_numpy().tolist()
    for brand_name, model_name in brand_model_list:
        max_min_query = read_sql_file(
            query_file_path="sql/get_max_min_data.sql",
            params={"DB_PRICE_TABLE": DB_PRICE_TABLE, "brand_name": brand_name, "model_name": model_name},
        )
        cursor.execute(max_min_query)
        max_min_data = cursor.fetchall()[0]
        car_type, model_url, min_msrp, max_msrp = max_min_data
        graph_dict = {
            "brandName": brand_name,  # add brand name
            "modelName": model_name,  # add model name
            "carType": car_type,  # add car type
            "modelUrl": model_url,  # add model url
            "minPrice": min_msrp,  # add min price
            "maxPrice": max_msrp,  # add model name
        }
        graph_query = read_sql_file(
            query_file_path="sql/get_ytd_graph_data.sql",
            params={"DB_PRICE_TABLE": DB_PRICE_TABLE, "brand_name": brand_name, "model_name": model_name},
        )
        cursor.execute(graph_query)
        graph_data = cursor.fetchall()
        graph_data = pd.DataFrame(graph_data, columns=["msrp", "create_timestamp"])
        graph_data["create_timestamp"] = pd.to_datetime(graph_data["create_timestamp"]).dt.date
        graph_data = graph_data.sort_values(by="create_timestamp", ascending=False)
        graph_dict["curPrice"] = graph_data["msrp"].iloc[0]  # add current price
        graph_dict["maxPriceYTD"] = graph_data["msrp"].max()  # add max price YTD
        graph_dict["minPriceYTD"] = graph_data["msrp"].min()  # add min price YTD
        graph_dict["avgPriceYTD"] = graph_data["msrp"].mean()  # add average price YTD
        graph_dict["changeYTD"] = graph_data.shape[0] - 1  # add price changes YTD

        # fill in current and last year data points
        max_id = graph_data["create_timestamp"].idxmax()
        max_date_msrp, max_date = graph_data.loc[max_id].to_numpy()
        if max_date != date.today():
            graph_data.loc[len(graph_data), ["msrp", "create_timestamp"]] = [max_date_msrp, date.today()]
        min_id = graph_data["create_timestamp"].idxmin()
        min_date_msrp, min_date = graph_data.loc[min_id].to_numpy()
        last_year = date.today() - relativedelta(days=365)
        if min_date != last_year:
            graph_data.loc[len(graph_data), ["msrp", "create_timestamp"]] = [min_date_msrp, last_year]

        # fill in gaps in graph data
        graph_data = graph_data.sort_values(by="create_timestamp", ascending=False).reset_index(drop=True)
        graph_data_copy = graph_data.copy()
        graph_data_copy[["last_msrp", "last_timestamp"]] = graph_data_copy[["msrp", "create_timestamp"]].shift(-1)
        graph_data_copy["msrp_diff"] = graph_data_copy["msrp"] - graph_data_copy["last_msrp"]
        graph_data_copy["date_diff"] = (
            pd.to_datetime(graph_data_copy["create_timestamp"]) - pd.to_datetime(graph_data_copy["last_timestamp"])
        ).dt.days
        for _, row in graph_data_copy.iterrows():
            if (row["date_diff"] > 1) & (row["msrp_diff"] != 0) & pd.notna(row["msrp_diff"]):
                graph_data.loc[len(graph_data), ["msrp", "create_timestamp"]] = [
                    row["last_msrp"],
                    row["create_timestamp"] - relativedelta(days=1),
                ]
        graph_data["create_timestamp"] = pd.to_datetime(graph_data["create_timestamp"]).dt.strftime("%Y-%m-%d")
        graph_data = graph_data.sort_values(by=["create_timestamp"], ascending=[True]).rename(
            columns={"create_timestamp": "x", "msrp": "y"}
        )
        graph_data = graph_data[["x", "y"]]
        graph_dict["graphData"] = graph_data.to_dict("records")  # add graph data

        # create graph data json and store in redis
        model_data_json = json.dumps(graph_dict)
        cache.set(f"{KEY_PREFIX}graph_{brand_name}_{model_name.replace(' ', '_')}", model_data_json)

    cursor.close()
    connection.close()
    return "ok"
