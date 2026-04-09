-- ===================================================================
-- АВТОМАТИЧЕСКИЙ СКРИПТ ДЛЯ РАЗВЕРТЫВАНИЯ ВСЕХ ОБЪЕКТОВ В CLICKHOUSE
-- База данных: std16_121
-- Хост: 192.168.214.206
-- Дата: 2026-04-09
-- ===================================================================

-- ===================================================================
-- 1. СОЗДАНИЕ БАЗЫ ДАННЫХ
-- ===================================================================
CREATE DATABASE IF NOT EXISTS std16_121;

-- Переключаемся на базу
USE std16_121;

-- ===================================================================
-- 2. СОЗДАНИЕ СЛОВАРЕЙ (DICTIONARIES)
-- ===================================================================

-- 2.1 Словарь каналов сбыта
DROP DICTIONARY IF EXISTS std16_121.ch_channel_dict;
CREATE DICTIONARY std16_121.ch_channel_dict
(
    `distr_chan` String,
    `txtsh` String
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(
    HOST '192.168.214.203' 
    PORT 5432 
    USER 'std16_121' 
    PASSWORD 'GcMCyfC6qFk1S6A7' 
    DB 'adb' 
    TABLE 'std16_121.channel'
))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(HASHED());

-- 2.2 Словарь цен
DROP DICTIONARY IF EXISTS std16_121.ch_price_dict;
CREATE DICTIONARY std16_121.ch_price_dict
(
    `material` String,
    `region` String,
    `distr_chan` String,
    `price` Decimal(15, 2)
)
PRIMARY KEY material, region, distr_chan
SOURCE(POSTGRESQL(
    HOST '192.168.214.203' 
    PORT 5432 
    USER 'std16_121' 
    PASSWORD 'GcMCyfC6qFk1S6A7' 
    DB 'adb' 
    TABLE 'std16_121.price'
))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

-- 2.3 Словарь товаров
DROP DICTIONARY IF EXISTS std16_121.ch_product_dict;
CREATE DICTIONARY std16_121.ch_product_dict
(
    `material` String,
    `matdirec` String,
    `brand` String,
    `txt` String
)
PRIMARY KEY material
SOURCE(POSTGRESQL(
    HOST '192.168.214.203' 
    PORT 5432 
    USER 'std16_121' 
    PASSWORD 'GcMCyfC6qFk1S6A7' 
    DB 'adb' 
    TABLE 'std16_121.product'
))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(HASHED());

-- 2.4 Словарь типов акций
DROP DICTIONARY IF EXISTS std16_121.ch_promo_types_dict;
CREATE DICTIONARY std16_121.ch_promo_types_dict
(
    `promo_type` String,
    `promo_type_name` String
)
PRIMARY KEY promo_type
SOURCE(POSTGRESQL(
    HOST '192.168.214.203' 
    PORT 5432 
    USER 'std16_121' 
    PASSWORD 'GcMCyfC6qFk1S6A7' 
    DB 'adb' 
    TABLE 'std16_121.promo_types'
))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(HASHED());

-- 2.5 Словарь регионов
DROP DICTIONARY IF EXISTS std16_121.ch_region_dict;
CREATE DICTIONARY std16_121.ch_region_dict
(
    `region` String,
    `txt` String
)
PRIMARY KEY region
SOURCE(POSTGRESQL(
    HOST '192.168.214.203' 
    PORT 5432 
    USER 'std16_121' 
    PASSWORD 'GcMCyfC6qFk1S6A7' 
    DB 'adb' 
    TABLE 'std16_121.region'
))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

-- 2.6 Словарь магазинов
DROP DICTIONARY IF EXISTS std16_121.ch_stores_dict;
CREATE DICTIONARY std16_121.ch_stores_dict
(
    `store_code` String,
    `store_name` String
)
PRIMARY KEY store_code
SOURCE(POSTGRESQL(
    HOST '192.168.214.203' 
    PORT 5432 
    USER 'std16_121' 
    PASSWORD 'GcMCyfC6qFk1S6A7' 
    DB 'adb' 
    TABLE 'std16_121.stores'
))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

-- ===================================================================
-- 3. ВНЕШНИЕ ТАБЛИЦЫ (подключение к Greenplum через PostgreSQL)
-- ===================================================================

-- 3.1 Таблица чеков
DROP TABLE IF EXISTS std16_121.ch_checks_ext;
CREATE TABLE std16_121.ch_checks_ext
(
    `receipt_item` String,
    `billnum` Int64,
    `billitem` Int32,
    `material` Int64,
    `store` String,
    `day` Int32,
    `month` Int32,
    `netval_with_vat` Float64,
    `qty` Int32,
    `netval` Float64,
    `tax` Float64,
    `check_count` Int32
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'checks', 'std16_121', 'GcMCyfC6qFk1S6A7');

-- 3.2 Таблица купонов
DROP TABLE IF EXISTS std16_121.ch_coupons_ext;
CREATE TABLE std16_121.ch_coupons_ext
(
    `store_code` String,
    `coupon_date` Date,
    `coupon_number` String,
    `promo_id` String,
    `product` String,
    `receipt_id` String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'coupons', 'std16_121', 'GcMCyfC6qFk1S6A7');

-- 3.3 Таблица план-факт январь 2021
DROP TABLE IF EXISTS std16_121.ch_plan_fact_ext_202101;
CREATE TABLE std16_121.ch_plan_fact_ext_202101
(
    `date` Date,
    `region_code` String,
    `matdirec` String,
    `distr_chan` String,
    `plan_quantity` Int64,
    `fact_quantity` Int64,
    `percent_complete` Float64,
    `top_material_code` String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'plan_fact_202101', 'std16_121', 'GcMCyfC6qFk1S6A7');

-- 3.4 Таблица план-факт февраль 2021
DROP TABLE IF EXISTS std16_121.ch_plan_fact_ext_202102;
CREATE TABLE std16_121.ch_plan_fact_ext_202102
(
    `date` Date,
    `region_code` String,
    `matdirec` String,
    `distr_chan` String,
    `plan_quantity` Int64,
    `fact_quantity` Int64,
    `percent_complete` Float64,
    `top_material_code` String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'plan_fact_202102', 'std16_121', 'GcMCyfC6qFk1S6A7');

-- 3.5 Таблица акций
DROP TABLE IF EXISTS std16_121.ch_promos_ext;
CREATE TABLE std16_121.ch_promos_ext
(
    `promo_id` String,
    `promo_name` String,
    `promo_type` String,
    `product` String,
    `discount_value` Float64
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'promos', 'std16_121', 'GcMCyfC6qFk1S6A7');

-- 3.6 Таблица магазинов
DROP TABLE IF EXISTS std16_121.ch_stores_ext;
CREATE TABLE std16_121.ch_stores_ext
(
    `store_code` String,
    `store_name` String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'stores', 'std16_121', 'GcMCyfC6qFk1S6A7');

-- 3.7 Таблица трафика
DROP TABLE IF EXISTS std16_121.ch_traffic_ext;
CREATE TABLE std16_121.ch_traffic_ext
(
    `plant` String,
    `date` Date,
    `time` String,
    `frame_id` String,
    `quantity` Int32
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'traffic', 'std16_121', 'GcMCyfC6qFk1S6A7');

-- ===================================================================
-- 4. ЛОКАЛЬНЫЕ ТАБЛИЦЫ (MergeTree)
-- ===================================================================

-- 4.1 Таблица чеков
DROP TABLE IF EXISTS std16_121.ch_checks;
CREATE TABLE std16_121.ch_checks
(
    `receipt_item` String,
    `billnum` Int64,
    `billitem` Int32,
    `material` Int64,
    `store` String,
    `day` Date,
    `month` Int32,
    `netval_with_vat` Float64,
    `qty` Int32,
    `netval` Float64,
    `tax` Float64,
    `check_count` Int32
)
ENGINE = MergeTree
ORDER BY store
SETTINGS index_granularity = 8192;

-- 4.2 Таблица купонов
DROP TABLE IF EXISTS std16_121.ch_coupons;
CREATE TABLE std16_121.ch_coupons
(
    `store_code` String,
    `coupon_date` Date,
    `coupon_number` String,
    `promo_id` String,
    `product` String,
    `receipt_id` String
)
ENGINE = MergeTree
ORDER BY coupon_number
SETTINGS index_granularity = 8192;

-- 4.3 Таблица план-факт
DROP TABLE IF EXISTS std16_121.ch_plan_fact;
CREATE TABLE std16_121.ch_plan_fact
(
    `date` Date,
    `region_code` String,
    `matdirec` String,
    `distr_chan` String,
    `plan_quantity` Int64,
    `fact_quantity` Int64,
    `percent_complete` Float64,
    `top_material_code` String
)
ENGINE = MergeTree
ORDER BY date
SETTINGS index_granularity = 8192;

-- 4.4 Таблица акций
DROP TABLE IF EXISTS std16_121.ch_promos;
CREATE TABLE std16_121.ch_promos
(
    `promo_id` String,
    `promo_name` String,
    `promo_type` String,
    `product` String,
    `discount_value` Float64
)
ENGINE = MergeTree
ORDER BY promo_id
SETTINGS index_granularity = 8192;

-- 4.5 Таблица трафика
DROP TABLE IF EXISTS std16_121.ch_traffic;
CREATE TABLE std16_121.ch_traffic
(
    `plant` String,
    `date` Date,
    `time` String,
    `frame_id` String,
    `quantity` Int32
)
ENGINE = MergeTree
ORDER BY plant
SETTINGS index_granularity = 8192;

-- ===================================================================
-- 5. РАСПРЕДЕЛЕННЫЕ ТАБЛИЦЫ (Distributed)
-- ===================================================================

-- 5.1 Распределенная таблица чеков
DROP TABLE IF EXISTS std16_121.ch_checks_distr;
CREATE TABLE std16_121.ch_checks_distr AS std16_121.ch_checks
ENGINE = Distributed('default_cluster', 'std16_121', 'ch_checks', rand());

-- 5.2 Распределенная таблица купонов
DROP TABLE IF EXISTS std16_121.ch_coupons_distr;
CREATE TABLE std16_121.ch_coupons_distr AS std16_121.ch_coupons
ENGINE = Distributed('default_cluster', 'std16_121', 'ch_coupons', rand());

-- 5.3 Распределенная таблица план-факт
DROP TABLE IF EXISTS std16_121.ch_plan_fact_distr;
CREATE TABLE std16_121.ch_plan_fact_distr AS std16_121.ch_plan_fact
ENGINE = Distributed('default_cluster', 'std16_121', 'ch_plan_fact', rand());

-- 5.4 Распределенная таблица акций
DROP TABLE IF EXISTS std16_121.ch_promos_distr;
CREATE TABLE std16_121.ch_promos_distr AS std16_121.ch_promos
ENGINE = Distributed('default_cluster', 'std16_121', 'ch_promos', rand());

-- 5.5 Распределенная таблица трафика
DROP TABLE IF EXISTS std16_121.ch_traffic_distr;
CREATE TABLE std16_121.ch_traffic_distr AS std16_121.ch_traffic
ENGINE = Distributed('default_cluster', 'std16_121', 'ch_traffic', rand());

-- ===================================================================
-- 6. ПРЕДСТАВЛЕНИЯ (VIEWS)
-- ===================================================================

-- 6.1 Представление финансовой аналитики по дням
DROP VIEW IF EXISTS std16_121.v_finance_daily;
CREATE VIEW std16_121.v_finance_daily AS
SELECT
    toDate(toString(c.day)) AS date,
    toYYYYMM(toDate(toString(c.day))) AS month,
    c.store AS store_code,
    dictGetString('std16_121.ch_stores_dict', 'store_name', c.store) AS store_name,
    COUNT(DISTINCT c.billnum) AS checks_count,
    SUM(c.qty) AS items_sold,
    SUM(c.netval_with_vat) AS revenue,
    SUM(c.tax) AS tax_amount,
    COALESCE(d.daily_discounts, 0) AS discount_amount,
    SUM(c.netval_with_vat) - COALESCE(d.daily_discounts, 0) AS revenue_after_discounts,
    COALESCE(t.traffic_count, 0) AS traffic_count
FROM std16_121.ch_checks c
LEFT JOIN (
    SELECT
        cp.coupon_date AS date,
        cp.store_code,
        SUM(p.discount_value) AS daily_discounts
    FROM std16_121.ch_coupons cp
    LEFT JOIN std16_121.ch_promos p ON cp.promo_id = p.promo_id
    WHERE p.discount_value IS NOT NULL
    GROUP BY cp.coupon_date, cp.store_code
) d ON toDate(toString(c.day)) = d.date AND c.store = d.store_code
LEFT JOIN (
    SELECT
        plant AS store_code,
        date,
        SUM(quantity) AS traffic_count
    FROM std16_121.ch_traffic
    GROUP BY plant, date
) t ON c.store = t.store_code AND toDate(toString(c.day)) = t.date
WHERE c.month >= 202101 AND c.month <= 202102
GROUP BY c.day, c.store, d.daily_discounts, t.traffic_count
ORDER BY c.day, c.store;

-- 6.2 Представление план-факт для дашборда
DROP VIEW IF EXISTS std16_121.v_plan_fact_dashboard;
CREATE VIEW std16_121.v_plan_fact_dashboard AS
SELECT
    pf.date,
    toYYYYMM(pf.date) AS month,
    pf.region_code,
    dictGetString('std16_121.ch_region_dict', 'txt', pf.region_code) AS region_name,
    pf.matdirec,
    pf.distr_chan,
    dictGetString('std16_121.ch_channel_dict', 'txtsh', pf.distr_chan) AS channel_name,
    pf.plan_quantity,
    pf.fact_quantity,
    pf.percent_complete,
    pf.top_material_code,
    dictGetString('std16_121.ch_product_dict', 'txt', pf.top_material_code) AS top_material_name
FROM std16_121.ch_plan_fact_distr pf;

-- 6.3 Представление ежедневной статистики по магазинам
DROP VIEW IF EXISTS std16_121.v_shop_daily;
CREATE VIEW std16_121.v_shop_daily AS
SELECT
    toDate(toString(ch.day)) AS date,
    ch.month,
    ch.store AS store_code,
    dictGetString('std16_121.ch_stores_dict', 'store_name', ch.store) AS store_name,
    COUNT(DISTINCT ch.billnum) AS checks_count,
    SUM(ch.qty) AS items_sold,
    SUM(ch.netval_with_vat) AS revenue,
    COALESCE(t.traffic_sum, 0) AS traffic_count
FROM std16_121.ch_checks ch
LEFT JOIN (
    SELECT
        plant AS store_code,
        date,
        SUM(quantity) AS traffic_sum
    FROM std16_121.ch_traffic
    GROUP BY plant, date
) t ON ch.store = t.store_code AND toDate(toString(ch.day)) = t.date
WHERE ch.month >= 202101 AND ch.month <= 202102
GROUP BY ch.day, ch.month, ch.store, t.traffic_sum
ORDER BY ch.day, ch.store;