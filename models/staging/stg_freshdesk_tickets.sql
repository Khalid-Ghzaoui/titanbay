with source as (
    select * from {{ source('titanbay_raw', 'freshdesk_tickets') }}
),

renamed as (
    select
        ticket_id,
        lower(trim(requester_email)) as requester_email,
        trim(requester_name) as requester_name,
        subject,
        status,
        priority,
        cast(created_at as timestamp) as created_at,
        cast(resolved_at as timestamp) as resolved_at,
        tags,
        partner_label,

        -- derived field: how long the ticket took to resolve in hours
        -- null where resolved_at is null (i.e. ticket not yet resolved)
        timestamp_diff(cast(resolved_at as timestamp), cast(created_at as timestamp), hour) as resolution_hours

    from source
)

select * from renamed