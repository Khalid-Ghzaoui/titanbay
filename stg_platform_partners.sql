version: 2

models:
  - name: stg_freshdesk_tickets
    description: >
      One row per support ticket raised via the Titanbay platform.
      Timestamps cast from string at source. resolution_hours is null
      for tickets not yet resolved.
    columns:
      - name: ticket_id
        tests:
          - unique
          - not_null
      - name: requester_email
        tests:
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['open', 'pending', 'resolved', 'closed']
      - name: priority
        tests:
          - accepted_values:
              values: ['low', 'medium', 'high', 'urgent']

  - name: stg_platform_investors
    description: One row per investor registered on the platform.
    columns:
      - name: investor_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
          - not_null
      - name: entity_id
        tests:
          - not_null

  - name: stg_platform_relationship_managers
    description: One row per relationship manager employed by a partner organisation.
    columns:
      - name: rm_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
          - not_null

  - name: stg_platform_entities
    description: One row per investing entity.
    columns:
      - name: entity_id
        tests:
          - unique
          - not_null
      - name: kyc_status
        tests:
          - accepted_values:
              values: ['approved', 'pending', 'expired', 'rejected']

  - name: stg_platform_partners
    description: One row per partner organisation.
    columns:
      - name: partner_id
        tests:
          - unique
          - not_null
      - name: partner_type
        tests:
          - accepted_values:
              values: ['wealth_manager', 'fund_manager', 'family_office']

  - name: stg_platform_fund_closes
    description: One row per fund close.
    columns:
      - name: close_id
        tests:
          - unique
          - not_null
      - name: close_status
        tests:
          - accepted_values:
              values: ['upcoming', 'completed', 'cancelled']