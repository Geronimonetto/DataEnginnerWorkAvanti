import aiohttp
import asyncio
import json
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any

@dataclass
class ExtractionContract:
    """
    Contrato que define como o motor de extração deve se comportar para uma API específica.
    """
    indicator_name: str
    url: str 
    safe_date: str 

class ExtractData:
    """
    O 'Motor'. Totalmente genérico e não sabe se está extraindo Dólar ou Selic.
    Só sabe bater num endpoint (na URL montada), respeitar os Retrys e Salvar.
    """
    def __init__(self, contract: ExtractionContract):
        self.contract = contract

    async def extractor(self, max_retries: int = 3, base_delay: int = 1) -> Optional[dict]:
        async with aiohttp.ClientSession() as session:
            for attempt in range(1, max_retries + 1):
                try:
                    async with session.get(self.contract.url) as response:
                        response.raise_for_status() 
                        data = await response.json()
                        return data
                        
                except aiohttp.ClientError as e:
                    print(f"⚠️ [{self.contract.indicator_name.upper()}] Falha na rede ({attempt}/{max_retries}): {e}")
                    if attempt == max_retries:
                        print(f"❌ [{self.contract.indicator_name.upper()}] Falha definitiva.")
                        return None
                    delay = base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    print(f"🛑 [{self.contract.indicator_name.upper()}] Erro grave: {e}")
                    return None

    def save_to_bronze(self, data: Any):
        """
        Salva o dado bruto exatamente como veio localmente em JSON - garantido que é RAW e intocado!
        """
        if not data:
            return

        project_root = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))

        target_path = project_root / "data" / "bronze" / "bcb" / self.contract.indicator_name
        target_path.mkdir(parents=True, exist_ok=True)
        
        file_path = target_path / f"{self.contract.safe_date}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            
        print(f"✅ Dado salvo em disco: {file_path}")

    def save_to_postgres(self, data: Any):
        """
        Salva os dados de Ingestão dentro do PostgreSQL para o dbt consumir.
        """
        if not data:
            return
            
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        conn_str = f"postgresql+psycopg2://airflow:airflow@{db_host}:5432/airflow"
        engine = create_engine(conn_str)

        if self.contract.indicator_name == "selic":
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame(data.get("value", []))
            
        if df.empty:
            print(f"⚠️ [{self.contract.indicator_name.upper()}] DataFrame vazio, nada para inserir.")
            return

        df['_inserted_at'] = pd.Timestamp.now()

        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
                print(f"✅ Conexão com Postgres ({db_host}) OK. Schema 'bronze' verificado.")
        except Exception as e:
            print(f"❌ Erro de Network/Conexão com Postgres: {e}")
            raise

        table_name = f"bcb_{self.contract.indicator_name}"
        df.to_sql(table_name, engine, schema="bronze", if_exists="append", index=False)
        
        print(f"🐘 [POSTGRES] {len(df)} linhas inseridas em bronze.{table_name} no host {db_host}")
