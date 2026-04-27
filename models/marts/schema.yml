version: 2

models:
  - name: mart_investor_support_activity
    description: >
      One row per investor. Combines investor, entity, and partner context
      with ticket activity metrics. Distinguishes between tickets raised
      directly by the investor and tickets raised on their behalf by a
      relationship manager. Primary model for answering which investors
      generate the most support demand.
    columns:
      - name: investor_id
        description: Unique identifier for the investor
        tests:
          - unique
          - not_null
      - name: direct_tickets
        description: Tickets raised directly by this investor
      - name: rm_raised_tickets
        description: >
          Tickets raised by this investor's relationship manager.
          Attributed to all investors under that RM — reflects RM workload,
          not individual investor demand.
      - name: total_tickets
        description: Sum of direct and RM-raised tickets
      - name: avg_resolution_hours
        description: Average resolution time in hours for direct tickets only. Null where investor has no direct tickets.
      - name: avg_resolution_hours_label
        description: Human-readable resolution time. Shows 'no direct tickets' where investor has never raised a ticket directly.
      - name: rm_attribution_note
        description: Flags where rm_raised_tickets is non-zero so analysts understand the attribution logic.

  - name: mart_fund_close_pressure
    description: >
      One row per fund close. Joins fund close context with ticket volume
      in the 28 days before each scheduled close date. Primary model for
      anticipating IS team pressure around close activity.
    columns:
      - name: close_id
        description: Unique identifier for the fund close
        tests:
          - unique
          - not_null
      - name: tickets_in_28d_pre_close
        description: Number of tickets raised in the 28 days before this close date
      - name: pre_close_pressure_rating
        description: >
          Forward-looking pressure rating for upcoming closes only.
          high pressure = more than 10 tickets in window.
          moderate pressure = 5 to 10 tickets.
          low pressure = fewer than 5 tickets.
          Null for completed and cancelled closes.