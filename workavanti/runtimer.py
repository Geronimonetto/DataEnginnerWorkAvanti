import asyncio
import time
from workavanti.extract_data import ExtractionContract, ExtractData
from workavanti.auditor import log_pipeline_run

def create_selic_contract(start_date_dd_mm_yyyy: str) -> ExtractionContract:
    """Fábrica de contrato para SELIC."""
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={start_date_dd_mm_yyyy}"
    safe_date = start_date_dd_mm_yyyy.replace('/', '-')
    
    return ExtractionContract(
        indicator_name="selic", 
        url=url, 
        safe_date=safe_date
    )

def create_ptax_contract(moeda: str, start_date_mm_dd_yyyy: str, end_date_mm_dd_yyyy: str) -> ExtractionContract:
    """Fábrica de contrato para a PTAX do BCB (OData)."""
    base_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    query = f"?@moeda='{moeda}'&@dataInicial='{start_date_mm_dd_yyyy}'&@dataFinalCotacao='{end_date_mm_dd_yyyy}'&$filter=tipoBoletim%20eq%20'Fechamento'&$format=json"
    
    safe_date = f"{start_date_mm_dd_yyyy}_to_{end_date_mm_dd_yyyy}"
    
    return ExtractionContract(
        indicator_name=moeda.casefold(), # ex: "usd"
        url=f"{base_url}{query}", 
        safe_date=safe_date
    )

async def run_extraction(contract: ExtractionContract):
    """Função avulsa do Motor. Extrai e imediatamente salva assincronamente."""
    print(f"🚀 Iniciando download para {contract.indicator_name.upper()}...")
    
    motor = ExtractData(contract)
    start_time = time.time()
    
    try:
        resultado = await motor.extractor()
        
        rows_returned = 0
        if resultado is not None:
            if contract.indicator_name == "selic":
                rows_returned = len(resultado)
            else:
                rows_returned = len(resultado.get("value", []))
                
        duration = time.time() - start_time
        
        if resultado is not None:

            motor.save_to_bronze(resultado)
            motor.save_to_postgres(resultado)
            
            log_pipeline_run(
                label=f"Ingestion {contract.indicator_name.upper()}",
                etapa="bronze",
                status="success",
                rows_returned=rows_returned,
                duration_s=duration
            )
        else:
            log_pipeline_run(
                label=f"Ingestion {contract.indicator_name.upper()}",
                etapa="bronze",
                status="error",
                rows_returned=0,
                duration_s=duration,
                error_msg="API retornou None"
            )
    except Exception as e:
        duration = time.time() - start_time
        log_pipeline_run(
            label=f"Ingestion {contract.indicator_name.upper()}",
            etapa="bronze",
            status="error",
            rows_returned=0,
            duration_s=duration,
            error_msg=str(e)
        )

async def main():
    print("=== ORQUESTRADOR INICIADO ===")
    
    c_selic = create_selic_contract("01/01/2024")
    c_usd   = create_ptax_contract("USD", "01-01-2024", "01-15-2024")
    c_eur   = create_ptax_contract("EUR", "01-01-2024", "01-15-2024")

    await asyncio.gather(
        run_extraction(c_selic),
        run_extraction(c_usd),
        run_extraction(c_eur)
    )
    
    print("=== TODAS AS EXTRAÇÕES FORAM BEM SUCEDIDAS ===")

if __name__ == "__main__":
    asyncio.run(main())