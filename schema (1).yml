with tickets as (
    select * from {{ ref('stg_freshdesk_tickets') }}
),

investors as (
    select * from {{ ref('stg_platform_investors') }}
),

relationship_managers as (
    select * from {{ ref('stg_platform_relationship_managers') }}
), 

-- Step 1: matching ticket requester email to known investor
investor_match as (
    select 
        t.ticket_id,
        t.requester_email,
        t.requester_name,
        t.subject,
        t.status,
        t.priority,
        t.created_at,
        t.resolved_at,
        t.resolution_hours,
        t.tags,
        t.partner_label,
        i.investor_id as matched_investor_id,
        i.entity_id as matched_entity_id,
        i.relationship_manager_id as matched_relationship_manager_id,
        'investor' as requester_type
    from tickets t
    inner join investors i
        on t.requester_email = i.email
), 

-- Step 2: matching remaining unmatched tickets to relationship managers
rm_match as (
    select 
        t.ticket_id,
        t.requester_email,
        t.requester_name,
        t.subject,
        t.status,
        t.priority,
        t.created_at,
        t.resolved_at,
        t.resolution_hours,
        t.tags,
        t.partner_label,
        cast(null as string) as matched_investor_id,
        cast(null as string) as matched_entity_id,
        rm.rm_id as matched_rm_id,
        'relationship_manager' as requester_type
    from tickets t
    inner join relationship_managers rm
        on t.requester_email = rm.email

    -- only include tickets that were not already matched to an investor in step 1
    where t.ticket_id not in (select ticket_id from investor_match)
),

-- Step 3: catching anything that wasn't matched to either investors or RMs
unmatched as (
    select 
        t.ticket_id,
        t.requester_email,
        t.requester_name,
        t.subject,
        t.status,
        t.priority,
        t.created_at,
        t.resolved_at,
        t.resolution_hours,
        t.tags,
        t.partner_label,
        cast(null as string) as matched_investor_id,
        cast(null as string) as matched_entity_id,
        cast(null as string) as matched_rm_id,
        'unknown' as requester_type
    from tickets t

    -- only include tickets that were not already matched to an investor in step 1 or RM in step 2
    where t.ticket_id not in (select ticket_id from investor_match)
    and t.ticket_id not in (select ticket_id from rm_match)
), 

-- Step 4: union everything together
final as (
    select * from investor_match
    union all
    select * from rm_match
    union all
    select * from unmatched
)
select * from final