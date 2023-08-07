{% snapshot orders_snapshot_timestamp %}

    {{
        config(
          target_schema='dev',
          strategy='check',
          unique_key='order_id',
          check_cols = ['order_status', 'updated_at'],
        )
    }}

    select * from {{ source('Instacart', 'orders') }}

{% endsnapshot %}