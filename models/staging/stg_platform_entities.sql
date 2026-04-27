with source as (
    select * from {{ source('titanbay_raw', 'platform_entities') }}
),

renamed as (
    select 
        entity_id,
        lower(trim(entity_name)) as entity_name,
        partner_id,
        entity_type,
        kyc_status
    from source
)

select * from renamed