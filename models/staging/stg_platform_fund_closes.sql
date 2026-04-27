with source as (
    select * from {{ source('titanbay_raw', 'platform_fund_closes') }}
),

renamed as ( 
    select 
        close_id, 
        fund_id,
        trim(fund_name) as fund_name,
        partner_id,
        close_number, 
        scheduled_close_date,
        close_status,
        total_committed_aum,
    from source
)

select * from renamed