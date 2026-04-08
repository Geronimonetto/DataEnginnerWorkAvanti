{% snapshot snap_eur %}
{{
    config(
      target_schema='gold_history',
      unique_key='data_referencia',
      strategy='check',
      check_cols=['valor', 'is_filled'],
    )
}}

SELECT
    3 as sk_indicador,
    data_referencia,
    valor,
    is_filled
FROM {{ ref('stg_eur') }}

{% endsnapshot %}
