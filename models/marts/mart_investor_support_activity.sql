with tickets as (
    select * from {{ ref('int_tickets_requester_resolved') }}
),

investors as (
    select * from {{ ref('int_investors_enriched') }}
),

-- aggregate ticket raised directly by investors
investor_tickets as (
    select 
        matched_investor_id as investor_id,
        count(*) as total_tickets,
        countif(status = 'open' or status = 'pending') as open_tickets,
        countif(status = 'resolved' or status = 'closed') as resolved_tickets,
        countif(priority = 'urgent') as urgent_priority_tickets,
        countif(priority = 'high') as high_priority_tickets,
        min(created_at) as first_ticket_created_at,
        max(created_at) as last_ticket_created_at,
        round(avg(resolution_hours), 2) as avg_resolution_hours
    from tickets
    where requester_type = 'investor'
        and matched_investor_id is not null
    group by 1
),

-- aggregate tickets raised by RMs on behalf of investors
-- an RM ticket is attributed to all investors that RM manages 
rm_tickets as (
    select 
        i.investor_id, 
        count(*) as rm_raised_tickets
    from tickets t
    inner join investors i
        on t.matched_relationship_manager_id = i.relationship_manager_id
    where t.requester_type = 'relationship_manager'
    group by 1
),

final as (
    select 
    -- investor context
    i.investor_id,
    i.investor_name, 
    i.email as investor_email,
    i.country,
    i.investor_registered_at,
    i.entity_id,
    i.entity_name,
    i.entity_type,
    i.kyc_status,
    i.partner_id,
    i.partner_name,
    i.partner_type,
    i.relationship_manager_id,
    i.relationship_manager_name,

    -- ticket activity context
    coalesce(it.total_tickets, 0) as direct_tickets,
    coalesce(rm.rm_raised_tickets, 0) as rm_raised_tickets,
    coalesce(it.total_tickets, 0) + coalesce(rm.rm_raised_tickets, 0) as total_tickets,
    coalesce(it.open_tickets, 0) as open_tickets,
    coalesce(it.resolved_tickets, 0) as resolved_tickets,
    coalesce(it.urgent_priority_tickets, 0) as urgent_priority_tickets,
    coalesce(it.high_priority_tickets, 0) as high_priority_tickets,
    it.first_ticket_created_at,
    it.last_ticket_created_at,
    case when it.total_tickets = 0 or it.total_tickets is null then null else it.avg_resolution_hours end as avg_resolution_hours,
    case when coalesce(it.total_tickets, 0) = 0 then 'no direct tickets' when it.avg_resolution_hours is null then 'unresolved' else cast(it.avg_resolution_hours as string) end as avg_resolution_hours_label,
    -- RM attribution note
    case when coalesce(rm.rm_raised_tickets, 0) > 0 then 'includes rm-raised tickets attributed to all investors under this rm' else null end as rm_attribution_note


from investors i
left join investor_tickets it
    on i.investor_id = it.investor_id
left join rm_tickets rm
    on i.investor_id = rm.investor_id
)

select * from final
