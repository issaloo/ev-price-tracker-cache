WITH min_max AS (
    SELECT
        brand_name,
        model_name,
        MIN(msrp) as min_msrp,
        MAX(msrp) as max_msrp,
    FROM
        $$DB_PRICE_TABLE$$
    WHERE
        brand_name = '$$brand_name$$' AND
        model_name = '$$model_name$$'
    GROUP BY
        brand_name, model_name),

info AS (SELECT 
    SELECT
        brand_name,
        model_name,
        car_type,
        model_url,
    FROM
        $$DB_PRICE_TABLE$$
    WHERE
        brand_name = '$$brand_name$$' AND
        model_name = '$$model_name$$'
    ORDER BY create_timestamp DESC    
    LIMIT 1
) 

SELECT
    in.car_type,
    in.model_url,
    mm.min_msrp,
    mm.max_msrp,
FROM
    info AS in LEFT JOIN
    min_max AS mm ON in.brand_name = mm.brand_name AND in.model_name = mm.model_name;
