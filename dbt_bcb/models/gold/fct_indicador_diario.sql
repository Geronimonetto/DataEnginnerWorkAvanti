{{ config(materialized='table') }}

WITH snapshot_union AS (
    SELECT * FROM {{ ref('snap_selic') }}
    UNION ALL
    SELECT * FROM {{ ref('snap_usd') }}
    UNION ALL
    SELECT * FROM {{ ref('snap_eur') }}
)

SELECT
    -- Surrogate key artificial gerada pela janela temporal de inserção do indicador
    ROW_NUMBER() OVER(ORDER BY sk_indicador, data_referencia, dbt_valid_from) as sk_fato,
    data_referencia,
    sk_indicador,
    valor,
    is_filled,
    -- Traduzindo as metadatas do DBT Snapshot SCD2 para as regras de negócio
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN true ELSE false END as is_current
FROM snapshot_union
