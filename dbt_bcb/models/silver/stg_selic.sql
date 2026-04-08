{{ config(materialized='view') }}

WITH calendar AS (
    -- Gera nosso calendário matemático entre nosso primeiro extrato até hoje
    SELECT dt::date as data_referencia
    FROM generate_series('2024-01-01'::timestamp, CURRENT_DATE::timestamp, '1 day'::interval) dt
),
raw_selic_all AS (
    SELECT 
        TO_DATE(data, 'DD/MM/YYYY') as data_referencia,
        CAST(valor AS DECIMAL(18,6)) as valor,
        ROW_NUMBER() OVER(PARTITION BY TO_DATE(data, 'DD/MM/YYYY') ORDER BY _inserted_at DESC) as rn
    FROM {{ source('bronze', 'bcb_selic') }}
),
raw_selic AS (
    SELECT data_referencia, valor
    FROM raw_selic_all
    WHERE rn = 1
),
joined AS (
    -- Cruzamos todas as datas (INCLUINDO FINAIS DE SEMANA) com os dias úteis.
    SELECT 
        c.data_referencia,
        r.valor,
        -- Essa contagem cumulativa nos ajuda agrupar finais de semana com a ultima sexta útil.
        COUNT(r.valor) OVER (ORDER BY c.data_referencia) as group_id,
        CASE WHEN r.valor IS NULL THEN true ELSE false END as is_filled
    FROM calendar c
    LEFT JOIN raw_selic r ON c.data_referencia = r.data_referencia
)
SELECT 
    data_referencia,
    -- O First Value resgata a sexta e propaga pra frente no fds. Isso é FORWARD FILL em SQL puro!
    FIRST_VALUE(valor) OVER (PARTITION BY group_id ORDER BY data_referencia) as valor,
    is_filled
FROM joined
ORDER BY data_referencia
