# std16_121
@sapiens.solutions
# Проект std16_121 — ETL/ELT пайплайн для Greenplum и Clickhouse

## 📁 Файлы проекта

 `greenplum.sql`  Развертывание всех объектов в Greenplum: схема, внешние таблицы, STAGE, PREP, целевые таблицы, функции загрузки, представления 
 `clickhouse.sql`  Развертывание всех объектов в Clickhouse: база данных, словари, внешние таблицы, локальные таблицы, распределенные таблицы, представления 
 `std16_121_main_dag.py`  DAG для Apache Airflow: автоматизация загрузки данных, подготовки, расчета витрин и синхронизации с Clickhouse 
 `Дашборд проекта` http://192.168.214.200:8088/superset/dashboard/p/dRkVprdQAev/