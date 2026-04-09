-- ===================================================================
-- АВТОМАТИЧЕСКИЙ СКРИПТ ДЛЯ РАЗВЕРТЫВАНИЯ ВСЕХ ОБЪЕКТОВ
-- Схема: std16_121
-- ===================================================================

DO $$
BEGIN
    RAISE NOTICE '=== НАЧАЛО РАЗВЕРТЫВАНИЯ СХЕМЫ std16_121 ===';
END $$;

-- ===================================================================
-- 1. СОЗДАНИЕ СХЕМЫ
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS std16_121;

DO $$
BEGIN
    RAISE NOTICE '✓ Схема std16_121 создана/существует';
END $$;

-- ===================================================================
-- 2. ВНЕШНИЕ ТАБЛИЦЫ (EXTERNAL) - gpfdist и PXF
-- ===================================================================

-- 2.1 gpfdist: checks.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_checks;
CREATE EXTERNAL TABLE std16_121.ext_checks (
    receipt_item varchar,
    billnum int8,
    billitem int4,
    material int8,
    store varchar,
    day int4,
    month int4,
    netval_with_vat numeric,
    qty int4,
    netval varchar,
    tax varchar,
    check_count int4
)
LOCATION ('gpfdist://172.16.128.86:8080/cheks.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.2 gpfdist: coupons.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_coupons;
CREATE EXTERNAL TABLE std16_121.ext_coupons (
    store_code varchar,
    coupon_date int4,
    coupon_number varchar,
    promo_id varchar,
    product varchar,
    receipt_id varchar
)
LOCATION ('gpfdist://172.16.128.86:8080/coupons.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.3 gpfdist: price.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_price;
CREATE EXTERNAL TABLE std16_121.ext_price (
    material varchar,
    region varchar,
    distr_chan varchar,
    price varchar
)
LOCATION ('gpfdist://172.16.128.86:8080/price.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.4 gpfdist: product.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_product;
CREATE EXTERNAL TABLE std16_121.ext_product (
    material varchar,
    asgrp varchar,
    brand varchar,
    matcateg varchar,
    matdirec varchar,
    txt varchar
)
LOCATION ('gpfdist://172.16.128.86:8080/product.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.5 gpfdist: promo_types.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_promo_types;
CREATE EXTERNAL TABLE std16_121.ext_promo_types (
    promo_type varchar,
    promo_type_name varchar
)
LOCATION ('gpfdist://172.16.128.86:8080/promo_types.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.6 gpfdist: promos.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_promos;
CREATE EXTERNAL TABLE std16_121.ext_promos (
    promo_id varchar,
    promo_name varchar,
    promo_type varchar,
    product varchar,
    discount_value varchar
)
LOCATION ('gpfdist://172.16.128.86:8080/promos.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.7 gpfdist: region.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_region;
CREATE EXTERNAL TABLE std16_121.ext_region (
    region varchar,
    txt varchar
)
LOCATION ('gpfdist://172.16.128.86:8080/region.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.8 gpfdist: stores.csv
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_stores;
CREATE EXTERNAL TABLE std16_121.ext_stores (
    store_code varchar,
    store_name varchar
)
LOCATION ('gpfdist://172.16.128.86:8080/stores.csv')
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"' header)
SEGMENT REJECT LIMIT 10 ROWS;

-- 2.9 PXF: bills_head (из Postgres)
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_bills_head;
CREATE EXTERNAL TABLE std16_121.ext_bills_head (
    billnum int8,
    plant varchar,
    calday date
)
LOCATION ('pxf://gp.bills_head?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- 2.10 PXF: bills_item (из Postgres)
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_bills_item;
CREATE EXTERNAL TABLE std16_121.ext_bills_item (
    billnum int8,
    billitem int8,
    material int8,
    qty int8,
    netval numeric,
    tax numeric,
    rpa_sat numeric,
    calday date
)
LOCATION ('pxf://gp.bills_item?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- 2.11 PXF: channel (из Postgres)
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_channel;
CREATE EXTERNAL TABLE std16_121.ext_channel (
    distr_chan varchar,
    txtsh text
)
LOCATION ('pxf://gp.chanel?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- 2.12 PXF: plan (из Postgres)
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_plan;
CREATE EXTERNAL TABLE std16_121.ext_plan (
    date date,
    region varchar,
    matdirec varchar,
    quantity int4,
    distr_chan varchar
)
LOCATION ('pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- 2.13 PXF: sales (из Postgres)
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_sales;
CREATE EXTERNAL TABLE std16_121.ext_sales (
    date date,
    region varchar,
    material varchar,
    distr_chan varchar,
    quantity int4,
    check_nm varchar,
    check_pos varchar
)
LOCATION ('pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- 2.14 PXF: traffic (из Postgres)
DROP EXTERNAL TABLE IF EXISTS std16_121.ext_traffic;
CREATE EXTERNAL TABLE std16_121.ext_traffic (
    plant varchar,
    date varchar,
    time varchar,
    frame_id varchar,
    quantity int4
)
LOCATION ('pxf://gp.traffic?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DO $$
BEGIN
    RAISE NOTICE '✓ Внешние таблицы созданы (14 шт.)';
END $$;

-- ===================================================================
-- 3. STAGING-ТАБЛИЦЫ (временные, под структуру источников)
-- ===================================================================

DROP TABLE IF EXISTS std16_121.stg_checks;
CREATE TABLE std16_121.stg_checks (
    receipt_item varchar,
    billnum int8,
    billitem int4,
    material int8,
    store varchar,
    day int4,
    month int4,
    netval_with_vat numeric,
    qty int4,
    netval varchar,
    tax varchar,
    check_count int4
) DISTRIBUTED BY (billnum, billitem);

DROP TABLE IF EXISTS std16_121.stg_coupons;
CREATE TABLE std16_121.stg_coupons (
    store_code varchar,
    coupon_date varchar,
    coupon_number varchar,
    promo_id varchar,
    product varchar,
    receipt_id varchar
) DISTRIBUTED BY (coupon_number);

DROP TABLE IF EXISTS std16_121.stg_price;
CREATE TABLE std16_121.stg_price (
    material varchar(20),
    region varchar(20),
    distr_chan varchar(100),
    price varchar(20)
) DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS std16_121.stg_product;
CREATE TABLE std16_121.stg_product (
    material varchar(20),
    asgrp varchar(20),
    brand varchar(20),
    matcateg varchar(10),
    matdirec varchar(10),
    txt varchar(500)
) DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS std16_121.stg_promo_types;
CREATE TABLE std16_121.stg_promo_types (
    promo_type varchar,
    promo_type_name varchar
) DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS std16_121.stg_promos;
CREATE TABLE std16_121.stg_promos (
    promo_id varchar,
    promo_name varchar,
    promo_type varchar,
    product varchar,
    discount_value varchar
) DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS std16_121.stg_region;
CREATE TABLE std16_121.stg_region (
    region varchar(20),
    txt varchar(100)
) DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS std16_121.stg_stores;
CREATE TABLE std16_121.stg_stores (
    store_code varchar,
    store_name varchar
) DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS std16_121.stg_bills_head;
CREATE TABLE std16_121.stg_bills_head (
    billnum int8,
    plant varchar(4),
    calday date
) DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS std16_121.stg_bills_item;
CREATE TABLE std16_121.stg_bills_item (
    billnum int8,
    billitem int8,
    material int8,
    qty int8,
    netval numeric(17,2),
    tax numeric(17,2),
    rpa_sat numeric(17,2),
    calday date
) DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS std16_121.stg_channel;
CREATE TABLE std16_121.stg_channel (
    distr_chan varchar,
    txtsh varchar
) DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS std16_121.stg_plan;
CREATE TABLE std16_121.stg_plan (
    date date,
    region varchar(20),
    matdirec varchar(20),
    quantity int4,
    distr_chan varchar(100)
) DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS std16_121.stg_sales;
CREATE TABLE std16_121.stg_sales (
    date date,
    region varchar(20),
    material varchar(20),
    distr_chan varchar(100),
    quantity int4,
    check_nm varchar(100),
    check_pos varchar(100)
) DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS std16_121.stg_traffic;
CREATE TABLE std16_121.stg_traffic (
    plant varchar(4),
    date_str varchar(10),
    time_str varchar(6),
    frame_id varchar(10),
    quantity int4
) DISTRIBUTED RANDOMLY;

DO $$
BEGIN
    RAISE NOTICE '✓ Staging-таблицы созданы (14 шт.)';
END $$;

-- ===================================================================
-- 4. ЦЕЛЕВЫЕ ТАБЛИЦЫ (ODS/DDS)
-- ===================================================================

-- 4.1 Факт: bills_head (с партиционированием)
DROP TABLE IF EXISTS std16_121.bills_head;
CREATE TABLE std16_121.bills_head (
    billnum int8 NOT NULL,
    plant varchar(4),
    calday date
)
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE(calday) (
    START ('2020-11-01'::date) END ('2021-03-01'::date) EVERY ('1 mon'::interval)
);

-- 4.2 Факт: bills_item (с партиционированием)
DROP TABLE IF EXISTS std16_121.bills_item;
CREATE TABLE std16_121.bills_item (
    billnum int8 NOT NULL,
    billitem int8 NOT NULL,
    material int8,
    qty int8,
    netval numeric(17,2),
    tax numeric(17,2),
    rpa_sat numeric(17,2),
    calday date
)
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE(calday) (
    START ('2020-11-01'::date) END ('2021-03-01'::date) EVERY ('1 mon'::interval)
);

-- 4.3 Факт: sales (с партиционированием)
DROP TABLE IF EXISTS std16_121.sales;
CREATE TABLE std16_121.sales (
    date date,
    region varchar(20),
    material varchar(20),
    distr_chan varchar(100),
    quantity int4,
    check_nm varchar(100) NOT NULL,
    check_pos varchar(100) NOT NULL
)
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
DISTRIBUTED BY (check_nm, check_pos)
PARTITION BY RANGE(date) (
    START ('2021-01-01'::date) END ('2021-08-01'::date) EVERY ('1 mon'::interval)
);

-- 4.4 Факт: plan (с партиционированием)
DROP TABLE IF EXISTS std16_121.plan;
CREATE TABLE std16_121.plan (
    date date NOT NULL,
    region varchar(20) NOT NULL,
    matdirec varchar(20) NOT NULL,
    quantity int4,
    distr_chan varchar(100) NOT NULL
)
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
DISTRIBUTED BY (date, region, matdirec, distr_chan)
PARTITION BY RANGE(date) (
    START ('2021-01-01'::date) END ('2021-08-01'::date) EVERY ('1 mon'::interval)
);

-- 4.5 Факт: traffic (с партиционированием)
DROP TABLE IF EXISTS std16_121.traffic;
CREATE TABLE std16_121.traffic (
    plant varchar(4),
    date date,
    time time,
    frame_id varchar(10),
    quantity int4
)
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
DISTRIBUTED BY (plant)
PARTITION BY RANGE(date) (
    START ('2021-01-01'::date) END ('2021-03-01'::date) EVERY ('1 mon'::interval)
);

-- 4.6 Факт: checks
DROP TABLE IF EXISTS std16_121.checks;
CREATE TABLE std16_121.checks (
    receipt_item varchar,
    billnum int8,
    billitem int4,
    material int8,
    store varchar,
    day date,
    month int4,
    netval_with_vat numeric,
    qty int4,
    netval numeric,
    tax numeric,
    check_count int4
)
DISTRIBUTED BY (billnum, billitem)
PARTITION BY RANGE(month) (
    START (202101) END (202103) EVERY (1)
);

-- 4.7 Факт: coupons
DROP TABLE IF EXISTS std16_121.coupons;
CREATE TABLE std16_121.coupons (
    store_code varchar,
    coupon_date date,
    coupon_number varchar,
    promo_id varchar,
    product varchar,
    receipt_id varchar
)
DISTRIBUTED BY (coupon_number)
PARTITION BY RANGE(coupon_date) (
    PARTITION p202101 START ('2021-01-01'::date) END ('2021-02-01'::date),
    PARTITION p202102 START ('2021-02-01'::date) END ('2021-03-01'::date),
    DEFAULT PARTITION p_default
);

-- 4.8 Факт: plan_fact_202101
DROP TABLE IF EXISTS std16_121.plan_fact_202101;
CREATE TABLE std16_121.plan_fact_202101 (
    date date,
    region_code varchar(20),
    matdirec varchar,
    distr_chan varchar(100),
    plan_quantity int8,
    fact_quantity int8,
    percent_complete numeric,
    top_material_code varchar(20)
)
DISTRIBUTED BY (date);

-- 4.9 Факт: plan_fact_202102
DROP TABLE IF EXISTS std16_121.plan_fact_202102;
CREATE TABLE std16_121.plan_fact_202102 (
    date date,
    region_code varchar(20),
    matdirec varchar,
    distr_chan varchar(100),
    plan_quantity int8,
    fact_quantity int8,
    percent_complete numeric,
    top_material_code varchar(20)
)
DISTRIBUTED BY (date);

-- 4.10 Факт: shop_metrics
DROP TABLE IF EXISTS std16_121.shop_metrics;
CREATE TABLE std16_121.shop_metrics (
    store_code varchar,
    store_name varchar,
    revenue numeric,
    discount_amount numeric,
    revenue_after_discount numeric,
    items_sold int8,
    checks_count int8,
    traffic_count int8,
    promo_items_count int8,
    discount_share_pct numeric,
    avg_items_per_check numeric,
    conversion_rate_pct numeric,
    avg_check_amount numeric,
    revenue_per_visitor numeric
)
DISTRIBUTED BY (store_code);

-- 4.11 Справочник: channel (реплицированный)
DROP TABLE IF EXISTS std16_121.channel;
CREATE TABLE std16_121.channel (
    distr_chan varchar(100) NOT NULL,
    txtsh varchar(10) NOT NULL,
    CONSTRAINT channel_pkey PRIMARY KEY (distr_chan)
)
DISTRIBUTED REPLICATED;

-- 4.12 Справочник: price (реплицированный)
DROP TABLE IF EXISTS std16_121.price;
CREATE TABLE std16_121.price (
    material varchar(20) NOT NULL,
    region varchar(20) NOT NULL,
    distr_chan varchar(100) NOT NULL,
    price numeric(10,2)
)
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
DISTRIBUTED REPLICATED;

-- 4.13 Справочник: product (реплицированный)
DROP TABLE IF EXISTS std16_121.product;
CREATE TABLE std16_121.product (
    material varchar(20) NOT NULL,
    asgrp varchar(20),
    brand varchar(20),
    matcateg varchar(10),
    matdirec varchar(10),
    txt varchar(500),
    CONSTRAINT product_pkey PRIMARY KEY (material)
)
DISTRIBUTED REPLICATED;

-- 4.14 Справочник: promo_types (реплицированный)
DROP TABLE IF EXISTS std16_121.promo_types;
CREATE TABLE std16_121.promo_types (
    promo_type varchar,
    promo_type_name varchar
)
DISTRIBUTED REPLICATED;

-- 4.15 Справочник: promos (реплицированный)
DROP TABLE IF EXISTS std16_121.promos;
CREATE TABLE std16_121.promos (
    promo_id varchar,
    promo_name varchar,
    promo_type varchar,
    product varchar,
    discount_value numeric(15,2)
)
DISTRIBUTED REPLICATED;

-- 4.16 Справочник: region (реплицированный)
DROP TABLE IF EXISTS std16_121.region;
CREATE TABLE std16_121.region (
    region varchar(20) NOT NULL,
    txt varchar(100) NOT NULL,
    CONSTRAINT region_pkey PRIMARY KEY (region)
)
DISTRIBUTED REPLICATED;

-- 4.17 Справочник: stores (реплицированный)
DROP TABLE IF EXISTS std16_121.stores;
CREATE TABLE std16_121.stores (
    store_code varchar,
    store_name varchar
)
DISTRIBUTED REPLICATED;

-- ===================================================================
-- 4.18 PREP-таблицы (промежуточный слой для преобразований)
-- ===================================================================

DROP TABLE IF EXISTS std16_121.prep_coupons;
CREATE TABLE std16_121.prep_coupons (
    store_code varchar,
    coupon_date date,
    coupon_number varchar,
    promo_id varchar,
    product varchar,
    receipt_id varchar
) DISTRIBUTED BY (coupon_number);

DROP TABLE IF EXISTS std16_121.prep_promos;
CREATE TABLE std16_121.prep_promos (
    promo_id varchar,
    promo_name varchar,
    promo_type varchar,
    product varchar,
    discount_value numeric(15,2)
) DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS std16_121.prep_price;
CREATE TABLE std16_121.prep_price (
    material varchar,
    region varchar,
    distr_chan varchar,
    price numeric(15,2)
) DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS std16_121.prep_traffic;
CREATE TABLE std16_121.prep_traffic (
    plant varchar,
    date date,
    time time,
    frame_id varchar,
    quantity int4
) DISTRIBUTED BY (plant, date);

DROP TABLE IF EXISTS std16_121.prep_checks;
CREATE TABLE std16_121.prep_checks (
    receipt_item varchar,
    billnum int8,
    billitem int4,
    material int8,
    store varchar,
    day date,
    month int4,
    netval_with_vat numeric,
    qty int4,
    netval numeric,
    tax numeric,
    check_count int4
) DISTRIBUTED BY (billnum, billitem);

-- Таблица курсоров
DROP TABLE IF EXISTS std16_121.load_cursors;
CREATE TABLE IF NOT EXISTS std16_121.load_cursors (
    table_name varchar(100),
    last_load_date timestamp,
    last_load_value text,
    rows_loaded bigint DEFAULT 0,
    status varchar(20),
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP
);

DO $$
BEGIN
    RAISE NOTICE '✓ Целевые таблицы созданы (17 шт.)';
END $$;


-- ===================================================================
-- 5. ФУНКЦИИ ЗАГРУЗКИ
-- ===================================================================

-- 5.1 Полная загрузка из внешней таблицы
DROP FUNCTION IF EXISTS std16_121.full_upl_ext_table(text, text, bool);
CREATE OR REPLACE FUNCTION std16_121.full_upl_ext_table(
    ext_table_from_name text,
    table_to_name text,
    truncate_bool bool DEFAULT true
)
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    from_schema text;
    from_table text;
    to_schema text;
    to_table text;
    columns_info record;
    column_count int := 0;
    table_exists boolean;
    columns_definition text := '';
    create_sql text;
    result_message text;
    insert_sql text;
    row_count int;
BEGIN
    from_schema := split_part(ext_table_from_name, '.', 1);
    from_table := split_part(ext_table_from_name, '.', 2);
    to_schema := split_part(table_to_name, '.', 1);
    to_table := split_part(table_to_name, '.', 2);
    
    IF from_table = '' THEN
        from_schema := 'public';
        from_table := ext_table_from_name;
    END IF;
    IF to_table = '' THEN
        to_schema := 'public';
        to_table := table_to_name;
    END IF;

    SELECT EXISTS(
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = to_schema AND c.relname = to_table AND c.relkind = 'r'
    ) INTO table_exists;

    FOR columns_info IN
        SELECT a.attname as column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = from_schema AND c.relname = from_table
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        column_count := column_count + 1;
        IF columns_definition = '' THEN
            columns_definition := format('%s %s', columns_info.column_name, columns_info.data_type);
        ELSE
            columns_definition := columns_definition || format(', %s %s', columns_info.column_name, columns_info.data_type);
        END IF;
    END LOOP;

    IF column_count = 0 THEN
        RETURN format('ОШИБКА: таблица %s.%s не найдена или не содержит колонок', from_schema, from_table);
    END IF;

    IF NOT table_exists THEN
        create_sql := format('CREATE TABLE %I.%I (%s) DISTRIBUTED RANDOMLY', to_schema, to_table, columns_definition);
        EXECUTE create_sql;
        result_message := format('СОЗДАНА новая таблица %s.%s с %s колонками', to_schema, to_table, column_count);
    ELSIF truncate_bool THEN
        EXECUTE format('TRUNCATE TABLE %I.%I', to_schema, to_table);
        result_message := format('ОЧИЩЕНА существующая таблица %s.%s', to_schema, to_table);
    ELSE
        result_message := format('ИСПОЛЬЗУЕТСЯ существующая таблица %s.%s (без очистки)', to_schema, to_table);
    END IF;

    insert_sql := format('INSERT INTO %I.%I SELECT * FROM %I.%I', to_schema, to_table, from_schema, from_table);
    EXECUTE insert_sql;
    GET DIAGNOSTICS row_count = ROW_COUNT;
    result_message := result_message || format(' и загружено %s строк', row_count);

    RETURN result_message;
END;
$$;

-- 5.2 Delta загрузка с подменой партиций
DROP FUNCTION IF EXISTS std16_121.delta_partition_load(text, text, text, bool);
CREATE OR REPLACE FUNCTION std16_121.delta_partition_load(
    p_source_table text,
    p_target_table text,
    p_year_month text,
    p_validation bool DEFAULT true
)
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    from_schema text;
    from_table text;
    to_schema text;
    to_table text;
    temp_table_name text;
    start_date date;
    end_date date;
    partition_date date;
    row_count int;
    validation_text text;
    exchange_sql text;
    add_partition_sql text;
    result_message text;
    partition_exists boolean;
    partition_name text;
    table_oid oid;
    dist_key text;
BEGIN
    from_schema := split_part(p_source_table, '.', 1);
    from_table := split_part(p_source_table, '.', 2);
    to_schema := split_part(p_target_table, '.', 1);
    to_table := split_part(p_target_table, '.', 2);
    
    IF from_table = '' THEN
        from_schema := 'public';
        from_table := p_source_table;
    END IF;
    IF to_table = '' THEN
        to_schema := 'public';
        to_table := p_target_table;
    END IF;
    
    start_date := to_date(p_year_month || '-01', 'YYYY-MM-DD');
    end_date := start_date + interval '1 month';
    partition_date := start_date + interval '5 days';
    
    SELECT c.oid INTO table_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = to_schema AND c.relname = to_table;
    
    IF table_oid IS NULL THEN 
        RETURN format('ОШИБКА: Таблица %s.%s не найдена', to_schema, to_table);
    END IF;
    
    SELECT pg_get_table_distributedby(table_oid) INTO dist_key;
    IF dist_key IS NULL OR dist_key = '' THEN 
        dist_key := 'DISTRIBUTED RANDOMLY';
    END IF;
    
    SELECT EXISTS(
        SELECT 1 FROM pg_partitions 
        WHERE schemaname = to_schema AND tablename = to_table 
          AND partitionboundary::text LIKE '%' || to_char(start_date, 'YYYY-MM-DD') || '%'
    ) INTO partition_exists;
    
    IF NOT partition_exists THEN
        partition_name := to_table || '_1_prt_' || to_char(start_date, 'YYYYMM');
        add_partition_sql := format('ALTER TABLE %I.%I ADD PARTITION %I START (%L) INCLUSIVE END (%L) EXCLUSIVE',
            to_schema, to_table, partition_name, start_date, end_date);
        EXECUTE add_partition_sql;
        result_message := format('СОЗДАНА новая партиция %s для месяца %s. ', partition_name, p_year_month);
    ELSE
        result_message := '';
    END IF;
    
    temp_table_name := to_table || '_tmp_' || replace(p_year_month, '-', '_');
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING STORAGE) %s',
        to_schema, temp_table_name, to_schema, to_table, dist_key);
    
    EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I WHERE date >= %L AND date < %L',
        to_schema, temp_table_name, from_schema, from_table, start_date, end_date);
    GET DIAGNOSTICS row_count = ROW_COUNT;
    
    IF row_count = 0 THEN
        EXECUTE format('DROP TABLE %I.%I', to_schema, temp_table_name);
        RETURN format('ОШИБКА: Нет данных для периода %s в таблице %s.%s', p_year_month, from_schema, from_table);
    END IF;
    
    validation_text := CASE WHEN p_validation THEN 'WITH VALIDATION' ELSE 'WITHOUT VALIDATION' END;
    exchange_sql := format('ALTER TABLE %I.%I EXCHANGE PARTITION FOR (%L) WITH TABLE %I.%I %s',
        to_schema, to_table, partition_date, to_schema, temp_table_name, validation_text);
    EXECUTE exchange_sql;
    EXECUTE format('DROP TABLE %I.%I', to_schema, temp_table_name);
    
    result_message := result_message || format('DELTA_PARTITION: обновлена партиция %s в таблице %s.%s, загружено %s строк, %s',
        p_year_month, to_schema, to_table, row_count, CASE WHEN p_validation THEN 'проверка границ включена' ELSE 'проверка границ отключена' END);
    
    RETURN result_message;
END;
$$;

-- 5.3 Delta загрузка с UPSERT
DROP FUNCTION IF EXISTS std16_121.delta_upsert_load(text, text, text, bool);
CREATE OR REPLACE FUNCTION std16_121.delta_upsert_load(
    p_source_table text,
    p_target_table text,
    p_key_columns text,
    p_truncate_first bool DEFAULT false,
    p_cursor_column text DEFAULT NULL  -- колонка для курсора (например 'date')
)
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    from_schema text;
    from_table text;
    to_schema text;
    to_table text;
    temp_table_name text;
    row_count int;
    deleted_count int;
    inserted_count int;
    create_temp_sql text;
    insert_temp_sql text;
    delete_sql text;
    insert_sql text;
    result_message text;
    table_oid oid;
    storage_params text;
    dist_key text;
    key_condition text;
    i int;
    key_array text[];
    -- Переменные для курсора
    last_cursor_value text;
    new_cursor_value text;
    has_cursor_column boolean := false;
    cursor_exists boolean;
BEGIN
    -- 1. Разбираем имена таблиц
    from_schema := split_part(p_source_table, '.', 1);
    from_table := split_part(p_source_table, '.', 2);
    to_schema := split_part(p_target_table, '.', 1);
    to_table := split_part(p_target_table, '.', 2);
    
    IF from_table = '' THEN
        from_schema := 'public';
        from_table := p_source_table;
    END IF;
    IF to_table = '' THEN
        to_schema := 'public';
        to_table := p_target_table;
    END IF;
    
    -- 2. Получаем OID целевой таблицы
    SELECT c.oid INTO table_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = to_schema AND c.relname = to_table;
    
    IF table_oid IS NULL THEN 
        RETURN format('ОШИБКА: Таблица %s.%s не найдена', to_schema, to_table);
    END IF;
    
    -- 3. Получаем параметры хранения
    SELECT array_to_string(reloptions,', ') INTO storage_params FROM pg_class WHERE oid = table_oid;
    IF storage_params IS NULL THEN storage_params := ''; ELSE storage_params := 'WITH (' || storage_params || ')'; END IF;
    
    -- 4. Получаем ключ распределения
    SELECT pg_get_table_distributedby(table_oid) INTO dist_key;
    IF dist_key IS NULL OR dist_key = '' THEN dist_key := 'DISTRIBUTED RANDOMLY'; END IF;
    
    -- 5. Проверяем, существует ли колонка курсора в исходной таблице
    IF p_cursor_column IS NOT NULL AND NOT p_truncate_first THEN
        SELECT EXISTS(
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = from_schema 
              AND table_name = from_table 
              AND column_name = p_cursor_column
        ) INTO has_cursor_column;
        
        IF has_cursor_column THEN
            -- Получаем последнее значение курсора
            SELECT EXISTS(
                SELECT 1 FROM std16_121.load_cursors 
                WHERE table_name = to_table
            ) INTO cursor_exists;
            
            IF cursor_exists THEN
                SELECT last_load_value INTO last_cursor_value
                FROM std16_121.load_cursors
                WHERE table_name = to_table;
            END IF;
            
            -- Если курсора нет, берем минимальную дату
            IF last_cursor_value IS NULL THEN
                last_cursor_value := '1900-01-01';
            END IF;
            
            RAISE NOTICE 'Курсор для таблицы %: колонка = %, значение = %', 
                         to_table, p_cursor_column, last_cursor_value;
        END IF;
    END IF;
    
    -- 6. Создаем временную таблицу
    temp_table_name := to_table || '_tmp_upsert_' || to_char(NOW(), 'YYYYMMDDHH24MISS');
    create_temp_sql := format('CREATE TABLE %I.%I (LIKE %I.%I) %s %s',
        to_schema, temp_table_name, to_schema, to_table, storage_params, dist_key);
    EXECUTE create_temp_sql;
    
    -- 7. Загружаем данные из источника (с учетом курсора)
    IF has_cursor_column AND NOT p_truncate_first AND last_cursor_value IS NOT NULL THEN
        insert_temp_sql := format('
            INSERT INTO %I.%I 
            SELECT * FROM %I.%I 
            WHERE %I > %L',
            to_schema, temp_table_name, 
            from_schema, from_table,
            p_cursor_column, last_cursor_value);
        RAISE NOTICE 'Инкрементальная загрузка: %I.%I где %I > %L', 
                     from_schema, from_table, p_cursor_column, last_cursor_value;
    ELSE
        insert_temp_sql := format('INSERT INTO %I.%I SELECT * FROM %I.%I',
            to_schema, temp_table_name, from_schema, from_table);
        RAISE NOTICE 'Полная загрузка: %I.%I', from_schema, from_table;
    END IF;
    
    EXECUTE insert_temp_sql;
    GET DIAGNOSTICS row_count = ROW_COUNT;
    
    IF row_count = 0 THEN
        EXECUTE format('DROP TABLE %I.%I', to_schema, temp_table_name);
        IF has_cursor_column THEN
            RETURN format('ИНФО: Нет новых данных в %s.%s (курсор: %s > %s)', 
                         from_schema, from_table, p_cursor_column, last_cursor_value);
        ELSE
            RETURN format('ОШИБКА: Нет данных в таблице %s.%s', from_schema, from_table);
        END IF;
    END IF;
    
    RAISE NOTICE 'Загружено % строк во временную таблицу', row_count;
    
    -- 8. UPSERT логика
    IF p_truncate_first THEN
        -- FULL LOAD
        EXECUTE format('TRUNCATE TABLE %I.%I', to_schema, to_table);
        insert_sql := format('INSERT INTO %I.%I SELECT * FROM %I.%I', 
            to_schema, to_table, to_schema, temp_table_name);
        EXECUTE insert_sql;
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        deleted_count := 0;
        result_message := format('FULL LOAD: таблица %s.%s очищена, загружено %s строк', 
            to_schema, to_table, inserted_count);
    ELSE
        -- UPSERT: удаляем по ключу
        key_array := string_to_array(p_key_columns, ',');
        key_condition := '';
        FOR i IN 1..array_length(key_array, 1) LOOP
            IF i > 1 THEN key_condition := key_condition || ' AND '; END IF;
            key_condition := key_condition || format('t.%s = s.%s', trim(key_array[i]), trim(key_array[i]));
        END LOOP;
        
        delete_sql := format('DELETE FROM %I.%I t USING %I.%I s WHERE %s',
            to_schema, to_table, to_schema, temp_table_name, key_condition);
        EXECUTE delete_sql;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        
        insert_sql := format('INSERT INTO %I.%I SELECT * FROM %I.%I', 
            to_schema, to_table, to_schema, temp_table_name);
        EXECUTE insert_sql;
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        
        result_message := format('UPSERT LOAD: таблица %s.%s, удалено %s строк, вставлено %s строк',
            to_schema, to_table, deleted_count, inserted_count);
    END IF;
    
    -- 9. Обновляем курсор (если колонка указана и не FULL LOAD)
    IF has_cursor_column AND NOT p_truncate_first AND inserted_count > 0 THEN
        EXECUTE format('SELECT MAX(%I)::text FROM %I.%I', 
                      p_cursor_column, to_schema, temp_table_name) INTO new_cursor_value;
        
        IF new_cursor_value IS NOT NULL THEN
            SELECT EXISTS(
                SELECT 1 FROM std16_121.load_cursors WHERE table_name = to_table
            ) INTO cursor_exists;
            
            IF cursor_exists THEN
                EXECUTE format('
                    UPDATE std16_121.load_cursors 
                    SET last_load_date = CURRENT_TIMESTAMP,
                        last_load_value = %L,
                        rows_loaded = rows_loaded + %L,
                        updated_at = CURRENT_TIMESTAMP,
                        status = %L
                    WHERE table_name = %L',
                    new_cursor_value, inserted_count, 'SUCCESS', to_table);
            ELSE
                EXECUTE format('
                    INSERT INTO std16_121.load_cursors 
                    (table_name, last_load_date, last_load_value, rows_loaded, updated_at, status)
                    VALUES (%L, CURRENT_TIMESTAMP, %L, %L, CURRENT_TIMESTAMP, %L)',
                    to_table, new_cursor_value, inserted_count, 'SUCCESS');
            END IF;
            
            RAISE NOTICE 'Курсор для таблицы % обновлен: % → %', 
                         to_table, last_cursor_value, new_cursor_value;
        END IF;
    END IF;
    
    -- 10. Удаляем временную таблицу
    EXECUTE format('DROP TABLE %I.%I', to_schema, temp_table_name);
    RETURN result_message;
    
EXCEPTION WHEN OTHERS THEN
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', to_schema, temp_table_name);
    RETURN format('ОШИБКА: %s', SQLERRM);
END;
$$;

-- 5.4 Функция расчета витрины plan_fact
DROP FUNCTION IF EXISTS std16_121.calculate_plan_fact(text);
CREATE OR REPLACE FUNCTION std16_121.calculate_plan_fact(p_year_month text)
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    target_table text;
    start_date date;
    end_date date;
    sql_text text;
    row_count int;
    result_msg text;
BEGIN
    start_date := to_date(p_year_month || '-01', 'YYYY-MM-DD');
    end_date := start_date + interval '1 month';
    target_table := 'plan_fact_' || replace(p_year_month, '-', '');
    
    sql_text := format('
        -- CASCADE удалит и зависимые представления (которые потом пересоздадутся)
        DROP TABLE IF EXISTS std16_121.%I CASCADE;
        
        CREATE TABLE std16_121.%I AS
        WITH 
        fact_agg AS (
            SELECT 
                s.date,
                s.region,
                p.matdirec,
                s.distr_chan,
                SUM(s.quantity) as fact_quantity
            FROM std16_121.sales s
            LEFT JOIN std16_121.product p ON s.material = p.material
            WHERE s.date >= %L AND s.date < %L
            GROUP BY s.date, s.region, p.matdirec, s.distr_chan
        ),
        plan_agg AS (
            SELECT 
                date,
                region,
                matdirec,
                distr_chan,
                SUM(quantity) as plan_quantity
            FROM std16_121.plan
            WHERE date >= %L AND date < %L
            GROUP BY date, region, matdirec, distr_chan
        ),
        top_product AS (
            SELECT DISTINCT ON (s.region)
                s.region,
                s.material as top_material
            FROM std16_121.sales s
            WHERE s.date >= %L AND s.date < %L
            GROUP BY s.region, s.material
            ORDER BY s.region, SUM(s.quantity) DESC
        )
        SELECT 
            COALESCE(f.date, p.date) as date,
            COALESCE(f.region, p.region) as region_code,
            COALESCE(f.matdirec, p.matdirec) as matdirec,
            COALESCE(f.distr_chan, p.distr_chan) as distr_chan,
            COALESCE(p.plan_quantity, 0) as plan_quantity,
            COALESCE(f.fact_quantity, 0) as fact_quantity,
            CASE 
                WHEN COALESCE(p.plan_quantity, 0) = 0 THEN 0
                ELSE ROUND(100.0 * COALESCE(f.fact_quantity, 0) / p.plan_quantity, 2)
            END as percent_complete,
            tp.top_material as top_material_code
        FROM fact_agg f
        FULL OUTER JOIN plan_agg p ON f.date = p.date 
                                   AND f.region = p.region 
                                   AND f.matdirec = p.matdirec 
                                   AND f.distr_chan = p.distr_chan
        LEFT JOIN top_product tp ON COALESCE(f.region, p.region) = tp.region
        ORDER BY date, region_code, matdirec, distr_chan',
        target_table, target_table, start_date, end_date, start_date, end_date, start_date, end_date);
    
    EXECUTE sql_text;
    
    EXECUTE format('SELECT COUNT(*) FROM std16_121.%I', target_table) INTO row_count;
    
    -- Пересоздаем представление (оно было удалено из-за CASCADE)
    sql_text := '
        CREATE OR REPLACE VIEW std16_121.v_plan_fact AS
        SELECT 
            pf.date,
            pf.region_code,
            r.txt as region_name,
            pf.matdirec,
            pf.distr_chan,
            c.txtsh as channel_name,
            pf.plan_quantity,
            pf.fact_quantity,
            pf.percent_complete,
            pf.top_material_code,
            pr.brand as top_material_brand,
            pr.txt as top_material_name,
            p.price as top_material_price
        FROM std16_121.' || target_table || ' pf
        LEFT JOIN std16_121.region r ON pf.region_code = r.region
        LEFT JOIN std16_121.channel c ON pf.distr_chan = c.distr_chan
        LEFT JOIN std16_121.product pr ON pf.top_material_code = pr.material
        LEFT JOIN std16_121.price p ON pf.top_material_code = p.material 
                                    AND pf.region_code = p.region 
                                    AND pf.distr_chan = p.distr_chan
        ORDER BY pf.date, pf.region_code, pf.matdirec, pf.distr_chan';
    
    EXECUTE sql_text;
    
    result_msg := format('Витрина std16_121.%s создана, строк: %s. Представление v_plan_fact обновлено', 
                         target_table, row_count);
    RETURN result_msg;
END;
$$;

DO $$
BEGIN
    RAISE NOTICE '✓ Функции созданы (4 шт.)';
END $$;

-- ===================================================================
-- 6. ПРЕДСТАВЛЕНИЯ (VIEW)
-- ===================================================================

-- 6.1 Основное представление v_plan_fact (будет создано/обновлено при вызове calculate_plan_fact)
-- Создаем заглушку, которая будет перезаписана при первом расчете витрины
CREATE OR REPLACE VIEW std16_121.v_plan_fact AS
SELECT 
    pf.date,
    pf.region_code,
    r.txt as region_name,
    pf.matdirec,
    pf.distr_chan,
    c.txtsh as channel_name,
    pf.plan_quantity,
    pf.fact_quantity,
    pf.percent_complete,
    pf.top_material_code,
    pr.brand as top_material_brand,
    pr.txt as top_material_name,
    p.price as top_material_price
FROM std16_121.plan_fact_202102 pf
LEFT JOIN std16_121.region r ON pf.region_code = r.region
LEFT JOIN std16_121.channel c ON pf.distr_chan = c.distr_chan
LEFT JOIN std16_121.product pr ON pf.top_material_code = pr.material
LEFT JOIN std16_121.price p ON pf.top_material_code = p.material 
                            AND pf.region_code = p.region 
                            AND pf.distr_chan = p.distr_chan
ORDER BY pf.date, pf.region_code, pf.matdirec, pf.distr_chan;

DO $$
BEGIN
    RAISE NOTICE '✓ Представления созданы (v_plan_fact)';
END $$;
