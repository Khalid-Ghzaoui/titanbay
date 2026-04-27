with investors as (
    select * from {{ ref('stg_platform_investors') }}
),

entities as (
    select * from {{ ref('stg_platform_entities') }}
),

partners as (
    select * from {{ ref('stg_platform_partners') }}
),

relationship_managers as (
    select * from {{ ref('stg_platform_relationship_managers') }}
),

final as (
    select 
        -- investor info
        i.investor_id,
        i.user_id,
        i.email,
        i.full_name as investor_name,
        i.country, 
        i.created_at as investor_registered_at,
        -- entity info
        e.entity_id,
        e.entity_name,
        e.entity_type,
        e.kyc_status,

        -- partner info
        p.partner_id,
        p.partner_name,
        p.partner_type,

        -- relationship manager info
        -- null where investor manages their own activity directly (~41% of investors)
        i.relationship_manager_id,
        rm.full_name as relationship_manager_name,
        rm.email as relationship_manager_email

    from investors i
    inner join entities e
        on i.entity_id = e.entity_id
    inner join partners p
        on e.partner_id = p.partner_id
    left join relationship_managers rm
        on i.relationship_manager_id = rm.rm_id
)

select * from final