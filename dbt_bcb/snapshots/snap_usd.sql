{% snapshot snap_usd %}
{{
    config(
      target_schema='gold_history',
      unique_key='data_referencia',
      strategy='check',
      check_cols=['valor', 'is_filled'],
    )
}}

SELECT
    2 as sk_indicador,
    data_referencia,
    valor,
    is_filled
FROM {{ ref('stg_usd') }}

{% endsnapshot %}
