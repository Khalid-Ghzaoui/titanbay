**Submitted by: Khalid el Ghzaoui**

# Titanbay Analytics Engineering Take-Home

## Business Problem

The Investor Services (IS) team at Titanbay handles support tickets reactively with no structured view of which investors generate the most tickets, what patterns exist, or when the team is likely to be under pressure. This model gives analysts the foundation to answer both questions.

## What an Analyst Can Now Do

- Rank investors by total ticket volume, distinguishing between tickets they raised directly and tickets raised on their behalf by a relationship manager
- Filter the investor base by KYC status, partner, entity type, or country to identify which segments drive the most support demand
- See the forward-looking pressure calendar — which upcoming fund closes are likely to generate high ticket volume based on historical patterns
- Identify the IS team's current live workload by filtering to open and pending tickets
- Understand RM workload concentration — which relationship managers are raising the most tickets and on behalf of how many investors

## Modelling Approach

The project follows a standard three-layer dbt architecture:

- **Staging** — one model per source table, light renaming and casting only, no business logic. Timestamps cast from string at this layer due to how the source CSVs were loaded.
- **Intermediate** — business logic and joins. Two models: entity resolution and investor enrichment.
- **Marts** — analyst-ready tables materialised as physical tables in BigQuery for query performance.

```
freshdesk_tickets ─────────────────────────────────────────┐
platform_investors ──┐                                     │
platform_entities ───┼── int_investors_enriched ───────────┼── mart_investor_support_activity
platform_partners ───┘                                     │
platform_relationship_managers ── int_tickets_requester_resolved ── mart_fund_close_pressure
platform_fund_closes ──────────────────────────────────────┘
```

## Entity Resolution

The core technical challenge in this task is that `freshdesk_tickets` contains only a `requester_email` with no platform identifier. The same email could belong to an investor raising a ticket directly, or a relationship manager raising a ticket on behalf of their clients.

`int_tickets_requester_resolved` resolves this using a priority-based email match:

1. Match `requester_email` against `platform_investors.email` → `requester_type = 'investor'`
2. For unmatched tickets, match against `platform_relationship_managers.email` → `requester_type = 'relationship_manager'`
3. Anything unmatched → `requester_type = 'unknown'`

**Results:**
| Requester Type | Tickets | Unique Requesters | Avg Tickets Each |
|---|---|---|---|
| investor | 1,060 | 318 | 3.3 |
| relationship_manager | 760 | 41 | 18.5 |
| unknown | 180 | 171 | 1.1 |

**Why this matters:** The top ticket raisers by raw email count are all relationship managers — jessica.white@aldgate-wealth.co.uk raised 59 tickets alone. Without entity resolution you would incorrectly identify these as problem investors. They are RMs doing their job, and their tickets need to be attributed to the underlying investors they manage.

## Key Data Quality Findings

**`partner_label` is unreliable as a join key.** The same partner appears as `Brockton Wealth Partners`, `FOXMORE INVESTOR PLATFORM`, `Brockton WP`, and `brockton wealth partners` across different tickets. This field is manually entered by the IS team and has a 44% null rate. It is not used as a join key anywhere in this model — email matching is the reliable path.

**Timestamps arrived as strings.** The `created_at` and `resolved_at` fields in `freshdesk_tickets` required explicit casting to TIMESTAMP at the staging layer. All other timestamp fields were clean.

**43% of investing entities have non-approved KYC status** — 114 pending, 111 rejected, 109 expired out of 772 total. This is likely a significant driver of support tickets and warrants further analysis.

**180 tickets (9%) could not be attributed** to any known investor or RM. Likely causes: investors who raised tickets before registering on the platform, external parties such as lawyers or fund administrators, or email mismatches between systems.

**845 tickets (42%) have no `resolved_at`** — consistent with the open and pending ticket count, expected and not a data quality issue.

## Assumptions

- **Entity resolution priority**: investor match takes precedence over RM match. If an email matches both, the requester is classified as an investor. This is an edge case but the assumption is documented.
- **RM ticket attribution**: when a ticket is raised by an RM, it is attributed to all investors under that RM via `relationship_manager_id`. This reflects RM workload but does not indicate which specific investor the ticket was about. `rm_raised_tickets` should be interpreted as an upper bound on RM-related demand per investor.
- **28-day pre-close window**: ticket pressure is measured in the 28 days before a scheduled close date. This assumption is based on the expectation that investor activity and questions increase during the commitment preparation period. The window is configurable.
- **Pressure thresholds**: `high pressure` is defined as more than 10 tickets in the window, `moderate pressure` as 5–10. These are starting assumptions and should be validated against IS team experience.

## Modelling Decisions

- **Staging materialised as views**: staging models are internal scaffolding queried only during dbt runs. No value in storing them as tables.
- **Intermediates materialised as views**: same reasoning — stepping stones, not analyst-facing.
- **Marts materialised as tables**: analyst-facing models that will be queried repeatedly. Physical tables avoid re-executing upstream logic on every query.
- **`coalesce(..., 0)` on all ticket metrics**: investors with no tickets appear in the mart with zeroes rather than nulls, making the output safe for aggregation and sorting without null handling.
- **Grain guards**: row counts were verified at each layer. `int_investors_enriched` and `mart_investor_support_activity` both return exactly 1,253 rows — matching the source investor count. `mart_fund_close_pressure` returns exactly 153 rows — matching the source fund closes count.

## What to Build Next

- **Tag analysis**: the `tags` field is a comma-separated string. Parsing it with `SPLIT()` and unnesting would enable ticket categorisation by issue type — e.g. how many tickets are KYC-related vs payment-related vs portal access.
- **Partner-level aggregation**: a `mart_partner_support_summary` model rolling up ticket volume, open tickets, and average resolution time by partner — useful for partner success conversations.
- **Time series model**: a daily or weekly ticket volume model joined to close dates would make the pressure relationship more visible and testable.
- **RM-level mart**: a dedicated model showing RM workload — tickets raised, investors managed, average resolution time — to help IS team triage and escalation.
- **Resolution time SLA tracking**: flag tickets that exceeded a resolution threshold (e.g. 72 hours) to identify systematic bottlenecks.

## How I Used AI

I used Claude throughout this task as a thinking partner and accelerator:

- **Architecture review**: validated the staged/intermediate/mart approach and grain decisions before writing SQL
- **Entity resolution logic**: worked through the priority-based email matching approach and edge cases
- **Data quality**: used Claude to help with interpreting some query results and surface findings from the raw data before building marts
- **SQL drafting**: used Claude to draft model SQL which I reviewed, corrected, and adapted to match my naming conventions and style
- **README structure**: used Claude to draft this README based on decisions and findings documented throughout the build

All SQL was reviewed and tested by me. The modelling decisions, assumptions, and trade-offs are my own.

## Reflection

The core linkage problem is that Freshdesk tickets contain only a free-text email address with no platform identifier, making entity resolution dependent on email matching which is brittle if emails change or differ between systems. The ideal long-term fix is to pass a `platform_user_id` at ticket creation time, surfacing the investor or RM's authenticated session identity directly into Freshdesk as a custom field. A secondary improvement would be enforcing structured partner tagging at ticket submission — replacing the manually entered `partner_label` with a validated dropdown tied to the platform's partner IDs — eliminating the inconsistency that makes it unusable as a join key today.

## Setup and Reproduction

### Prerequisites
- Python 3.9+
- A GCP project with BigQuery API enabled
- `gcloud` CLI installed and authenticated

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/Khalid-Ghzaoui/titanbay 
cd titanbay

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Load source tables into BigQuery
# Update project_id and downloads path in scripts/load_raw_tables.py first
python scripts/load_raw_tables.py

# 5. Configure dbt profile
# Copy the profiles.yml example below into ~/.dbt/profiles.yml
# Update project to your GCP project ID

# 6. Run dbt
dbt build
```

### profiles.yml example

```yaml
titanbay:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: your-gcp-project-id
      dataset: titanbay_dev
      threads: 4
      timeout_seconds: 300
```
