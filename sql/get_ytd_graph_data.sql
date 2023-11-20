SELECT
    msrp,
    create_timestamp
FROM
    $$DB_PRICE_TABLE$$
WHERE
    brand_name = '$$brand_name$$' AND
    model_name = '$$model_name$$' AND
    create_timestamp >= (CURRENT_DATE - interval '1 year');