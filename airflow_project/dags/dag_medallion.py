from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import asyncio
import os

from workavanti.runtimer import run_extraction, create_selic_contract, create_ptax_contract

# Data de início do histórico na primeira carga
HISTORIAL_START = "01/01/2024"
HISTORIAL_START_PTAX = "01-01-2024"

def sync_runner(contract):
    asyncio.run(run_extraction(contract))

def _is_first_load(table_name: str) -> bool:
    """Verifica se já existe dados na tabela bronze para decidir entre carga completa ou incremental."""
    from sqlalchemy import create_engine, text, inspect
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    engine = create_engine(f"postgresql+psycopg2://airflow:airflow@{db_host}:5432/airflow")
    insp = inspect(engine)
    if not insp.has_table(table_name, schema="bronze"):
        return True
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM bronze.{table_name}"))
        return result.scalar() == 0

def _yesterday_str(fmt: str) -> str:
    """Retorna a data de ontem (ou último dia útil) formatada. Garante que não pedimos data futura à API."""
    dt = datetime.utcnow() - timedelta(days=1)
    return dt.strftime(fmt)

@dag(
    dag_id="pipeline_medalhao_bcb",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["engenharia-dados", "bcb", "camada-bronze"],
    default_args={
        "owner": "Engenheiro de Dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)
def bcb_pipeline():
    
    @task(task_id="extrair_selic_bronze")
    def bronze_selic(**context):
        if _is_first_load("bcb_selic"):
            print("📦 Primeira carga SELIC — buscando histórico completo desde 01/01/2024")
            dt_start = HISTORIAL_START
        else:
            # Carga incremental: busca últimos 7 dias pra cobrir feriados/fins de semana
            dt = datetime.utcnow() - timedelta(days=7)
            dt_start = dt.strftime("%d/%m/%Y")
            print(f"📅 Carga incremental SELIC — buscando a partir de {dt_start}")
        c_selic = create_selic_contract(dt_start)
        sync_runner(c_selic)
        
    @task(task_id="extrair_usd_bronze")
    def bronze_usd(**context):
        if _is_first_load("bcb_usd"):
            print("📦 Primeira carga USD — buscando histórico completo desde 01/01/2024")
            dt_start = HISTORIAL_START_PTAX
        else:
            dt_start = (datetime.utcnow() - timedelta(days=7)).strftime("%m-%d-%Y")
            print(f"📅 Carga incremental USD — buscando desde {dt_start}")
        dt_end = _yesterday_str("%m-%d-%Y")
        c_usd = create_ptax_contract("USD", dt_start, dt_end)
        sync_runner(c_usd)

    @task(task_id="extrair_eur_bronze")
    def bronze_eur(**context):
        if _is_first_load("bcb_eur"):
            print("📦 Primeira carga EUR — buscando histórico completo desde 01/01/2024")
            dt_start = HISTORIAL_START_PTAX
        else:
            dt_start = (datetime.utcnow() - timedelta(days=7)).strftime("%m-%d-%Y")
            print(f"📅 Carga incremental EUR — buscando desde {dt_start}")
        dt_end = _yesterday_str("%m-%d-%Y")
        c_eur = create_ptax_contract("EUR", dt_start, dt_end)
        sync_runner(c_eur)

    dbt_seed = BashOperator(
        task_id="dbt_medallion_seed",
        bash_command="cd /opt/workavanti/dbt_bcb && dbt seed"
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_silver_layer",
        bash_command="cd /opt/workavanti/dbt_bcb && dbt run --select silver"
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_history_tracking",
        bash_command="cd /opt/workavanti/dbt_bcb && dbt snapshot"
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_gold_layer",
        bash_command="cd /opt/workavanti/dbt_bcb && dbt run --select gold"
    )
    
    export_csv = BashOperator(
        task_id="export_entregaveis",
        bash_command="cd /opt/workavanti && python workavanti/export_csv.py"
    )

    bronze_tasks = [bronze_selic(), bronze_usd(), bronze_eur()]
    
    bronze_tasks >> dbt_seed >> dbt_run_silver >> dbt_snapshot >> dbt_run_gold >> export_csv

dag_instance = bcb_pipeline()
