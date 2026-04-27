with fund_closes as (
    select * from {{ ref('stg_platform_fund_closes') }}
),

tickets as (
    select * from {{ ref('int_tickets_requester_resolved') }}
),

-- count tickets raised in the 28 days before each scheduled close date 
-- assumption: ticket volume spikes in the run-up to a close, not after 
-- 28 day window chosen to capture the full pre-close preparation period 
ticket_pressure as (
    select 
        fc.close_id,
        count(t.ticket_id) as tickets_in_28d_pre_close,
        countif(t.priority = 'urgent') as urgent_tickets_in_28d_pre_close,
        countif(t.priority = 'high') as high_priority_tickets_in_28d_pre_close,
        countif(t.requester_type = 'investor') as investor_tickets_in_28d_pre_close,
        countif(t.requester_type = 'relationship_manager') as rm_tickets_in_28d_pre_close
    from fund_closes fc
    left join tickets t 
        on t.created_at >= timestamp_sub(timestamp(fc.scheduled_close_date), interval 28 day)
        and t.created_at < timestamp(fc.scheduled_close_date)
        and t.partner_label is not null
    group by 1
), 

final as (
    select 
        -- final close context
        fc.close_id,
        fc.fund_id,
        fc.fund_name,
        fc.partner_id,
        fc.close_number,
        fc.scheduled_close_date,
        fc.close_status,
        fc.total_committed_aum,

        -- how many tickets were raised in the 28 days before this close 
        -- proxy for IS team pressure at close time 
        coalesce(tp.tickets_in_28d_pre_close, 0)            as tickets_in_28d_pre_close,
        coalesce(tp.urgent_tickets_in_28d_pre_close, 0)     as urgent_tickets_in_28d_pre_close,
        coalesce(tp.high_priority_tickets_in_28d_pre_close, 0)       as high_priority_tickets_in_28d_pre_close,
        coalesce(tp.investor_tickets_in_28d_pre_close, 0)   as investor_tickets_in_28d_pre_close,
        coalesce(tp.rm_tickets_in_28d_pre_close, 0)         as rm_tickets_in_28d_pre_close,

        -- forward-looking pressure rating for upcoming closes only 
        -- thresholds of 5 and 10 tickets are assumptions based on observed data 
        -- null for completed and cancelled closes as presuure is historical only 
        case 
            when fc.close_status = 'upcoming' and coalesce(tp.tickets_in_28d_pre_close, 0) > 10 then 'high pressure'
            when fc.close_status = 'upcoming' and coalesce(tp.tickets_in_28d_pre_close, 0) between 5 and 10 then 'moderate pressure'
            when fc.close_status = 'upcoming' then 'low pressure'
            else null
        end as pre_close_pressure_rating
    from fund_closes fc
    left join ticket_pressure tp
        on fc.close_id = tp.close_id
) 

select * from final


