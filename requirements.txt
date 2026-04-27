with source as (
    select * from {{ source('titanbay_raw', 'platform_relationship_managers') }}
),

renamed as (
    select
        rm_id,
        partner_id,
        trim(name) as full_name,
        lower(trim(email)) as email

    from source
)

select * from renamed