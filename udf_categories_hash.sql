CREATE OR REPLACE FUNCTION udf_categories_hash() RETURNS VOID AS
$$
BEGIN
    UPDATE dmstage.categories
    SET hash_key = MD5(CONCAT(UPPER(COALESCE(CAST(LTRIM(RTRIM(id)) AS VARCHAR(8000)), '')), '|',
                              COALESCE(UPPER(CAST(LTRIM(RTRIM(name)) AS VARCHAR(8000))), '')))
    WHERE hash_key IS NULL;
END;
$$ LANGUAGE plpgsql;