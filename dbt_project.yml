with source as (
    select * from {{ source('titanbay_raw', 'platform_investors') }}   
), 

renamed as (
    select 
        investor_id,
        user_id, 
        lower(trim(email)) as email,
        lower(trim(full_name)) as full_name,
        entity_id, 
        country, 
        created_at, 
        relationship_manager_id
    from source
)

select * from renamed

