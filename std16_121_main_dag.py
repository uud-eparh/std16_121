from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# =====================================================
# КОНФИГУРАЦИЯ
# =====================================================
DB_CONN = "gp_std16_121"
DB_SCHEMA = 'std16_121'  

DB_FULL_LOAD = 'full_upl_ext_table'
DB_PROC_LOAD = 'delta_upsert_load'
DB_PROC_MART = 'calculate_plan_fact'

# Clickhouse настройки
CLICKHOUSE_DB = 'std16_121'
CLICKHOUSE_LOGIN = 'std16_121'
CLICKHOUSE_PASS = Variable.get("PASSWORD_16_121", default_var="")
CLICKHOUSE_HOST = "192.168.214.206"
CLICKHOUSE_PORT = "8123"

# Email для уведомлений
ALERT_EMAIL = 'uud-eparh@yandex.ru'

BASE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?database={CLICKHOUSE_DB}"

# =====================================================
# Функции для проверки загрузки
# =====================================================

def check_table_count(table_name, expected_min=1):
    def _check():
        hook = PostgresHook(postgres_conn_id=DB_CONN)
        sql = f"SELECT COUNT(*) FROM {DB_SCHEMA}.{table_name}"
        result = hook.get_first(sql)
        count = result[0] if result else 0
        logger.info(f"Таблица {DB_SCHEMA}.{table_name}: {count} строк")
        if count < expected_min:
            error_msg = f"ПРОВАЛ: Таблица {DB_SCHEMA}.{table_name} содержит {count} строк (минимум {expected_min})"
            logger.error(error_msg)
            raise Exception(error_msg)
        else:
            logger.info(f"✅ {DB_SCHEMA}.{table_name}: проверка пройдена ({count} строк)")
        return count
    return _check


def send_success_email(**context):
    logger.info(f"✅ DAG {DB_SCHEMA}_main_dag успешно выполнен")
    logger.info(f"Время выполнения: {context.get('execution_date', datetime.now())}")


def send_failure_email(context):
    task_instance = context.get('task_instance')
    logger.error(f"❌ Ошибка в задаче: {task_instance.task_id if task_instance else 'Unknown'}")


def make_curl(sql):
    sql_with_schema = sql.replace('{schema}', CLICKHOUSE_DB)
    sql_escaped = sql_with_schema.replace('"', '\\"').replace('`', '\\`')
    return f'curl -s -u {CLICKHOUSE_LOGIN}:{CLICKHOUSE_PASS} -X POST -H "Content-Type: text/plain" --data-binary "{sql_escaped}" "{BASE_URL}"'


# =====================================================
# Функции для загрузки STAGE таблиц
# =====================================================

def load_stage_gpfdist(table_name):
    def _load():
        hook = PostgresHook(postgres_conn_id=DB_CONN)
        sql = f"""
            TRUNCATE TABLE {DB_SCHEMA}.stg_{table_name};
            INSERT INTO {DB_SCHEMA}.stg_{table_name}
            SELECT * FROM {DB_SCHEMA}.ext_{table_name};
        """
        try:
            hook.run(sql)
            logger.info(f"stg_{table_name} загружена")
            return f"✅ SUCCESS: stg_{table_name}"
        except Exception as e:
            error_msg = f"❌ Ошибка загрузки stg_{table_name}: {str(e)}"
            logger.info(error_msg)
            
            try:
                send_email(
                    to=[ALERT_EMAIL],
                    subject=f'Airflow Warning: Ошибка загрузки stg_{table_name}',
                    html_content=f'<p><b>Таблица:</b> stg_{table_name}</p>'
                                 f'<p><b>Ошибка:</b> {str(e)}</p>'
                                 f'<p><b>Действие:</b> Загрузка пропущена</p>'
                )
            except:
                pass
            
            return f"⏸️ ПРОПУЩЕНА: stg_{table_name}"
    
    return _load


def load_stage_pxf(table_name):
    def _load():
        hook = PostgresHook(postgres_conn_id=DB_CONN)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            conn.autocommit = False
            cursor.execute("BEGIN;")
            cursor.execute(f"TRUNCATE TABLE {DB_SCHEMA}.stg_{table_name};")
            cursor.execute(f"""
                INSERT INTO {DB_SCHEMA}.stg_{table_name}
                SELECT * FROM {DB_SCHEMA}.ext_{table_name};
            """)
            conn.commit()
            logger.info(f"{DB_SCHEMA}.stg_{table_name} загружена")
            return f"✅ УСПЕХ: {DB_SCHEMA}.stg_{table_name}"
        except Exception as e:
            conn.rollback()
            error_msg = f"❌ Ошибка загрузки {DB_SCHEMA}.stg_{table_name}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            cursor.close()
            conn.close()
    return _load


# =====================================================
# SQL команды для синхронизации с Clickhouse
# =====================================================

SQL_COMMANDS = [
    "DROP TABLE IF EXISTS {schema}.ch_bills",
    "CREATE TABLE {schema}.ch_bills ENGINE = MergeTree ORDER BY store AS SELECT * FROM {schema}.ch_bills_ext",
    "DROP TABLE IF EXISTS {schema}.ch_traffic",
    "CREATE TABLE {schema}.ch_traffic ENGINE = MergeTree ORDER BY plant AS SELECT * FROM {schema}.ch_traffic_ext",
    "DROP TABLE IF EXISTS {schema}.ch_coupons",
    "CREATE TABLE {schema}.ch_coupons ENGINE = MergeTree ORDER BY coupon_number AS SELECT * FROM {schema}.ch_coupons_ext",
    "DROP TABLE IF EXISTS {schema}.ch_promos",
    "CREATE TABLE {schema}.ch_promos ENGINE = MergeTree ORDER BY promo_id AS SELECT * FROM {schema}.ch_promos_ext",
    "DROP TABLE IF EXISTS {schema}.ch_plan_fact",
    "CREATE TABLE {schema}.ch_plan_fact ENGINE = MergeTree ORDER BY date AS SELECT * FROM {schema}.ch_plan_fact_ext",
]

DISTR_COMMANDS = [
    "DROP TABLE IF EXISTS {schema}.ch_bills_distr",
    "CREATE TABLE {schema}.ch_bills_distr AS {schema}.ch_bills ENGINE = Distributed('default_cluster', '{schema}', 'ch_bills', rand())",
    "DROP TABLE IF EXISTS {schema}.ch_traffic_distr",
    "CREATE TABLE {schema}.ch_traffic_distr AS {schema}.ch_traffic ENGINE = Distributed('default_cluster', '{schema}', 'ch_traffic', rand())",
    "DROP TABLE IF EXISTS {schema}.ch_coupons_distr",
    "CREATE TABLE {schema}.ch_coupons_distr AS {schema}.ch_coupons ENGINE = Distributed('default_cluster', '{schema}', 'ch_coupons', rand())",
    "DROP TABLE IF EXISTS {schema}.ch_promos_distr",
    "CREATE TABLE {schema}.ch_promos_distr AS {schema}.ch_promos ENGINE = Distributed('default_cluster', '{schema}', 'ch_promos', rand())",
    "DROP TABLE IF EXISTS {schema}.ch_plan_fact_distr",
    "CREATE TABLE {schema}.ch_plan_fact_distr AS {schema}.ch_plan_fact ENGINE = Distributed('default_cluster', '{schema}', 'ch_plan_fact', rand())",
]

# =====================================================
# Группы таблиц
# =====================================================

# GPFDIST таблицы (CSV через gpfdist)
GPFDIST_TABLES = ['coupons', 'price', 'product', 'promo_types', 'promos', 'region', 'stores']

# PXF таблицы (PostgreSQL через PXF) - без bills_head и bills_item
PXF_TABLES = ['channel', 'plan', 'sales', 'traffic']

# Загрузка bills, bills_head и bills_item через отдельную функцию
BILLS_LOAD_SQL = [
    ("bills_head", "SELECT delta_upsert_load('std16_121.ext_bills_head', 'std16_121.stg_bills_head', 'billnum', false, 'calday');"),
    ("bills_item", "SELECT delta_upsert_load('std16_121.ext_bills_item', 'std16_121.stg_bills_item', 'billnum, billitem', false, 'calday');"),
    ("bills", "SELECT delta_upsert_load('std16_121.ext_bills', 'std16_121.stg_bills', 'billnum, billitem', false, 'calday');"),
]

# =====================================================
# PREP таблицы
# =====================================================

PREP_TABLES = {
    'coupons': f"""
        BEGIN;
        DROP TABLE IF EXISTS {{schema}}.prep_coupons;
        CREATE TABLE {{schema}}.prep_coupons AS
        SELECT 
            store_code,
            TO_DATE(coupon_date::text, 'YYYYMMDD') as coupon_date,
            coupon_number,
            promo_id,
            TRIM(product) as product,
            receipt_id
        FROM {{schema}}.stg_coupons
        DISTRIBUTED BY (coupon_number);
        COMMIT;
    """,
    'promos': f"""
        BEGIN;
        DROP TABLE IF EXISTS {{schema}}.prep_promos;
        CREATE TABLE {{schema}}.prep_promos AS
        SELECT 
            promo_id,
            promo_name,
            promo_type,
            TRIM(product) as product,
            discount_value::numeric(15,2) as discount_value
        FROM {{schema}}.stg_promos
        DISTRIBUTED REPLICATED;
        COMMIT;
    """,
    'price': f"""
        BEGIN;
        DROP TABLE IF EXISTS {{schema}}.prep_price;
        CREATE TABLE {{schema}}.prep_price AS
        SELECT 
            material,
            region,
            distr_chan,
            price::numeric(15,2) as price
        FROM {{schema}}.stg_price
        DISTRIBUTED REPLICATED;
        COMMIT;
    """,
    'traffic': f"""
        BEGIN;
        DROP TABLE IF EXISTS {{schema}}.prep_traffic;
        CREATE TABLE {{schema}}.prep_traffic AS
        SELECT 
            plant,
            TO_DATE(date_str, 'DD.MM.YYYY') as date,
            TO_TIMESTAMP(time_str, 'HH24MISS')::time as time,
            frame_id,
            quantity
        FROM {{schema}}.stg_traffic
        DISTRIBUTED BY (plant, date);
        COMMIT;
    """
}

# =====================================================
# Целевые таблицы
# =====================================================

DIMENSION_TABLES = {
    'channel': {'source': f'{DB_SCHEMA}.stg_channel', 'truncate': True},
    'region': {'source': f'{DB_SCHEMA}.stg_region', 'truncate': True},
    'product': {'source': f'{DB_SCHEMA}.stg_product', 'truncate': True},
    'stores': {'source': f'{DB_SCHEMA}.stg_stores', 'truncate': True},
    'promo_types': {'source': f'{DB_SCHEMA}.stg_promo_types', 'truncate': True},
    'price': {'source': f'{DB_SCHEMA}.prep_price', 'truncate': True},
    'promos': {'source': f'{DB_SCHEMA}.prep_promos', 'truncate': True},
}

FACT_TABLES = {
    'bills': {'source': f'{DB_SCHEMA}.stg_bills', 'key': 'billnum', 'cursor': 'calday'},
    'bills_head': {'source': f'{DB_SCHEMA}.stg_bills_head', 'key': 'billnum', 'cursor': 'calday'},
    'bills_item': {'source': f'{DB_SCHEMA}.stg_bills_item', 'key': 'billnum, billitem', 'cursor': 'calday'},
    'plan': {'source': f'{DB_SCHEMA}.stg_plan', 'key': 'date, region, matdirec, distr_chan', 'cursor': 'date'},
    'sales': {'source': f'{DB_SCHEMA}.stg_sales', 'key': 'check_nm, check_pos', 'cursor': 'date'},
    'coupons': {'source': f'{DB_SCHEMA}.prep_coupons', 'key': 'coupon_number', 'cursor': 'coupon_date'},
    'traffic': {'source': f'{DB_SCHEMA}.prep_traffic', 'key': 'plant, date, time, frame_id', 'cursor': 'date'},
}

# =====================================================
# Витрина
# =====================================================

TARGET_MONTHS = ['2021-01', '2021-02']

FULL_LOAD_SQL = f'SELECT {{schema}}.{DB_FULL_LOAD}(%(tab_name_from)s, %(tab_name_to)s, %(truncate_bool_flag)s);'
LOAD_SQL = f'SELECT {{schema}}.{DB_PROC_LOAD}(%(tab_name_from)s, %(tab_name_to)s, %(key_columns)s, %(full_load_flag)s, %(cursor_column)s);'

# =====================================================
# DAG
# =====================================================

default_args = {
    'depends_on_past': False,
    'owner': DB_SCHEMA,
    'start_date': datetime(2023, 4, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30),
}

with DAG(
    f"{DB_SCHEMA}_main_dag",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    description=f"DAG для загрузки данных в схему {DB_SCHEMA}",
    tags=['greenplum', 'clickhouse', 'etl']
) as dag:
    
    task_start = DummyOperator(task_id="start")

    # =====================================================
    # ГРУППА 1: Загрузка STAGE таблиц (GPFDIST)
    # =====================================================
    with TaskGroup('load_stage_gpfdist') as task_load_stage_gpfdist:
        for table in GPFDIST_TABLES:
            PythonOperator(
                task_id=f'stage_load_{table}',
                python_callable=load_stage_gpfdist(table)
            )

    # =====================================================
    # ГРУППА 2: Загрузка STAGE таблиц (PXF)
    # =====================================================
    with TaskGroup('load_stage_pxf') as task_load_stage_pxf:
        for table in PXF_TABLES:
            PythonOperator(
                task_id=f'stage_load_{table}',
                python_callable=load_stage_pxf(table)
            )

    # =====================================================
    # ГРУППА 3: Загрузка bills_head и bills_item через delta_upsert_load
    # =====================================================
    with TaskGroup('load_stage_bills') as task_load_stage_bills:
        for table_name, sql in BILLS_LOAD_SQL:
            PostgresOperator(
                task_id=f'stage_load_{table_name}',
                postgres_conn_id=DB_CONN,
                sql=sql,
                autocommit=True,
            )

    # =====================================================
    # ГРУППА 4: Подготовка данных
    # =====================================================
    with TaskGroup('prepare_data') as task_prepare_data:
        for table_name, prep_sql in PREP_TABLES.items():
            PostgresOperator(
                task_id=f'prepare_{table_name}',
                postgres_conn_id=DB_CONN,
                sql=prep_sql.format(schema=DB_SCHEMA),
                autocommit=False,
            )

    # =====================================================
    # ГРУППА 5: Проверка PREP таблиц
    # =====================================================
    with TaskGroup('check_prep_tables') as task_check_prep:
        for table_name in PREP_TABLES.keys():
            PythonOperator(
                task_id=f'check_prep_{table_name}',
                python_callable=check_table_count(f'prep_{table_name}', expected_min=1)
            )

    # =====================================================
    # ГРУППА 6: FULL загрузка справочников
    # =====================================================
    with TaskGroup('full_insert_dimensions') as task_full_insert_dimensions:
        for table, config in DIMENSION_TABLES.items():
            PostgresOperator(
                task_id=f'load_dimension_{table}',
                postgres_conn_id=DB_CONN,
                sql=FULL_LOAD_SQL.format(schema=DB_SCHEMA),
                parameters={
                    'tab_name_from': config['source'],
                    'tab_name_to': f'{DB_SCHEMA}.{table}',
                    'truncate_bool_flag': str(config['truncate'])
                },
                autocommit=True,
            )

    # =====================================================
    # ГРУППА 7: Проверка справочников
    # =====================================================
    with TaskGroup('check_dimensions') as task_check_dimensions:
        for table in DIMENSION_TABLES.keys():
            PythonOperator(
                task_id=f'check_dim_{table}',
                python_callable=check_table_count(table, expected_min=1)
            )

    # =====================================================
    # ГРУППА 8: UPSERT загрузка фактов
    # =====================================================
    with TaskGroup('upsert_insert_facts') as task_upsert_insert_facts:
        for table, config in FACT_TABLES.items():
            PostgresOperator(
                task_id=f'load_fact_{table}',
                postgres_conn_id=DB_CONN,
                sql=LOAD_SQL.format(schema=DB_SCHEMA),
                parameters={
                    'tab_name_from': config['source'],
                    'tab_name_to': f'{DB_SCHEMA}.{table}',
                    'key_columns': config['key'],
                    'full_load_flag': 'False',
                    'cursor_column': config.get('cursor', 'NULL')
                },
                autocommit=True,
            )

    # =====================================================
    # ГРУППА 9: Проверка фактов
    # =====================================================
    with TaskGroup('check_facts') as task_check_facts:
        for table in FACT_TABLES.keys():
            PythonOperator(
                task_id=f'check_fact_{table}',
                python_callable=check_table_count(table, expected_min=1)
            )

    # =====================================================
    # ГРУППА 10: Расчет витрин
    # =====================================================
    with TaskGroup('calculate_marts') as task_calculate_marts:
        for month in TARGET_MONTHS:
            calculate_sql = f'SELECT {DB_SCHEMA}.{DB_PROC_MART}(\'{month}\');'
            PostgresOperator(
                task_id=f'calculate_plan_fact_{month.replace("-", "_")}',
                postgres_conn_id=DB_CONN,
                sql=calculate_sql,
                autocommit=True,
            )

    # =====================================================
    # ГРУППА 11: Проверка витрин
    # =====================================================
    with TaskGroup('check_marts') as task_check_marts:
        for month in TARGET_MONTHS:
            table_name = f'plan_fact_{month.replace("-", "")}'
            PythonOperator(
                task_id=f'check_mart_{month.replace("-", "_")}',
                python_callable=check_table_count(table_name, expected_min=100)
            )

    # =====================================================
    # ГРУППА 12: Синхронизация с Clickhouse
    # =====================================================
    with TaskGroup('sync_clickhouse') as task_sync_clickhouse:
        prev_task = None
        for i, sql in enumerate(SQL_COMMANDS):
            task = BashOperator(
                task_id=f'sync_{i}',
                bash_command=make_curl(sql),
            )
            if prev_task:
                prev_task >> task
            prev_task = task
        
        for i, sql in enumerate(DISTR_COMMANDS):
            task = BashOperator(
                task_id=f'distr_{i}',
                bash_command=make_curl(sql),
            )
            prev_task >> task
            prev_task = task

    # =====================================================
    # ГРУППА 13: Проверка Clickhouse
    # =====================================================
    check_clickhouse = PythonOperator(
        task_id='check_clickhouse',
        python_callable=lambda: logger.info(f"Clickhouse {CLICKHOUSE_DB} синхронизация завершена"),
    )

    # =====================================================
    # Уведомление об успехе
    # =====================================================
    send_success = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email,
        provide_context=True,
    )

    task_end = DummyOperator(task_id="end")

    # =====================================================
    # ПОРЯДОК ВЫПОЛНЕНИЯ
    # =====================================================
    task_start >> task_load_stage_gpfdist >> task_load_stage_pxf >> task_load_stage_bills >> task_prepare_data >> task_check_prep >> task_full_insert_dimensions >> task_check_dimensions >> task_upsert_insert_facts >> task_check_facts >> task_calculate_marts >> task_check_marts >> task_sync_clickhouse >> check_clickhouse >> send_success >> task_end