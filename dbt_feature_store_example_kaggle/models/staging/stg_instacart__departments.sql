with source as (
    select * from {{ source('instacart_raw', 'DEPARTMENTS') }}
),

renamed as (
    select
        DEPARTMENT_ID as department_id,
        DEPARTMENT as department_name
    from source
)

select * from renamed