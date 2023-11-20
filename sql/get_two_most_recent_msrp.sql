WITH rank_msrp AS (
    SELECT
        brand_name,
        model_name,
        msrp,
        image_src,
        model_url,
        ROW_NUMBER() OVER (PARTITION BY brand_name, model_name ORDER BY create_timestamp DESC) as rank
    FROM
        $$DB_PRICE_TABLE$$)

SELECT
    *
FROM
    rank_msrp
WHERE 
    rank <= 2;