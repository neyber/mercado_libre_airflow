CREATE OR REPLACE FUNCTION datamart.udf_load_categories() RETURNS VOID AS
$$
    BEGIN
        UPDATE datamart.dim_categories
        SET cat_end_date = CURRENT_TIMESTAMP,
            cat_current_record_flag = '0'
        WHERE EXISTS(
            SELECT 1
            FROM dmstage.categories cat
            WHERE cat.hash_key <> dim_categories.cat_hash_key
            AND cat.id = dim_categories.cat_id
        );

        INSERT INTO datamart.dim_categories (cat_id, cat_name, cat_load_date, cat_end_date, cat_current_record_flag, cat_hash_key)
        SELECT id, name, load_date, NULL, 1, hash_key
        FROM dmstage.categories
        WHERE NOT EXISTS(
            SELECT 1
            FROM datamart.dim_categories dcat
            WHERE dcat.cat_hash_key = categories.hash_key
            AND dcat.cat_current_record_flag = '1'
        );
    END;
$$ LANGUAGE plpgsql;