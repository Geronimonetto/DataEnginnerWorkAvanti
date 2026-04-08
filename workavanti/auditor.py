import uuid
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine

def log_pipeline_run(label: str, etapa: str, status: str, rows_returned: int, duration_s: float, error_msg: str = ""):
    """Registra uma execução do pipeline na tabela pipeline_runs.
    
    Colunas conforme spec: id_run, label, etapa, status, rows_returned, duration_s, executed_at, error_msg
    """
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    engine = create_engine(f"postgresql+psycopg2://airflow:airflow@{db_host}:5432/airflow")
    
    df = pd.DataFrame([{
        "id_run": str(uuid.uuid4()),
        "label": label,
        "etapa": etapa,
        "status": status,
        "rows_returned": rows_returned,
        "duration_s": round(duration_s, 2),
        "executed_at": datetime.now(),
        "error_msg": error_msg if error_msg else None
    }])
    
    df.to_sql("pipeline_runs", engine, schema="public", if_exists="append", index=False)
