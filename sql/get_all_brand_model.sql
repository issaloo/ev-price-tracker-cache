SELECT 
    DISTINCT 
        brand_name, 
        model_name 
FROM 
    $$DB_PRICE_TABLE$$
ORDER BY
    brand_name, 
    model_name;
