with source as (
    select * from {{ source('titanbay_raw', 'platform_partners') }}
), 

renamed as (
    select 
        partner_id,
        trim(partner_name) as partner_name,
        partner_type
    from source
)

select * from renamed