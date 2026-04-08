{{ config(materialized='view') }}

WITH calendar AS (
    SELECT dt::date as data_referencia
    FROM generate_series('2024-01-01'::timestamp, CURRENT_DATE::timestamp, '1 day'::interval) dt
),
raw_ptax_all AS (
    SELECT 
        CAST("dataHoraCotacao" AS DATE) as data_referencia,
        CAST("cotacaoCompra" AS DECIMAL(18,6)) as valor,
        ROW_NUMBER() OVER(PARTITION BY CAST("dataHoraCotacao" AS DATE) ORDER BY _inserted_at DESC) as rn
    FROM {{ source('bronze', 'bcb_usd') }}
),
raw_ptax AS (
    SELECT data_referencia, valor
    FROM raw_ptax_all
    WHERE rn = 1
),
joined AS (
    SELECT 
        c.data_referencia,
        r.valor,
        COUNT(r.valor) OVER (ORDER BY c.data_referencia) as group_id,
        CASE WHEN r.valor IS NULL THEN true ELSE false END as is_filled
    FROM calendar c
    LEFT JOIN raw_ptax r ON c.data_referencia = r.data_referencia
)
SELECT 
    data_referencia,
    FIRST_VALUE(valor) OVER (PARTITION BY group_id ORDER BY data_referencia) as valor,
    is_filled
FROM joined
ORDER BY data_referencia
