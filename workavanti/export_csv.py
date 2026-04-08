import pandas as pd
import os
from sqlalchemy import create_engine

def export_deliverables():
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    engine = create_engine(f"postgresql+psycopg2://airflow:airflow@{db_host}:5432/airflow")
    
    # Criar pasta data se não existir
    os.makedirs("data", exist_ok=True)
    
    # ─── 1. Exportar fct_indicador_diario (Gold) ───
    df_fct = pd.read_sql("SELECT * FROM public_gold.fct_indicador_diario", engine)
    
    # Padronizar booleans como lowercase (true/false)
    for col in ["is_filled", "is_current"]:
        if col in df_fct.columns:
            df_fct[col] = df_fct[col].map({True: "true", False: "false"})
    
    # Padronizar valor com 6 casas decimais fixas (ex: 10.650000, 4.871300)
    if "valor" in df_fct.columns:
        df_fct["valor"] = df_fct["valor"].apply(lambda x: f"{x:.6f}" if pd.notna(x) else "")
    
    df_fct.to_csv("data/fct_indicador_diario.csv", index=False)
    
    # ─── 2. Exportar dim_indicador ───
    df_dim = pd.read_sql("SELECT * FROM public.dim_indicador", engine)
    df_dim.to_csv("data/dim_indicador.csv", index=False)
    
    # ─── 3. Exportar pipeline_runs (apenas colunas da spec) ───
    # Colunas exigidas: id_run, label, etapa, status, rows_returned, duration_s, executed_at, error_msg
    spec_columns = ["id_run", "label", "etapa", "status", "rows_returned", "duration_s", "executed_at", "error_msg"]
    
    df_logs = pd.read_sql("SELECT * FROM public.pipeline_runs", engine)
    
    # Selecionar apenas as colunas da spec (ignorar colunas extras como endpoint, status_http)
    available_cols = [c for c in spec_columns if c in df_logs.columns]
    df_logs = df_logs[available_cols]
    
    # Formatar duration_s como string com "s" (ex: "1.2s")
    if "duration_s" in df_logs.columns:
        df_logs["duration_s"] = df_logs["duration_s"].apply(lambda x: f"{x}s" if pd.notna(x) else "")
    
    df_logs.to_csv("data/pipeline_runs.csv", index=False)
    
    print("✅ Entregáveis (CSVs) gerados com sucesso na pasta data/")

if __name__ == "__main__":
    export_deliverables()
