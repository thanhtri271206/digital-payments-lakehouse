# Engineering Review Checklist

## Data correctness
- Grain is explicit
- Keys/duplicates understood
- Incremental logic is correct
- Late changes are handled
- Schema evolution has a strategy

## Reliability
- Retry behavior understood
- Rerun is safe
- Idempotency/backfill considered
- Partial failure behavior understood

## Quality
- Critical assertions exist
- Freshness/completeness/validity checks exist where relevant
- Bad data behavior is defined

## Operations
- Logs/metrics exist where useful
- Alerts/visibility are proportional
- Run history/debugging is possible

## Security/cost
- Secrets are not hard-coded
- Least privilege considered
- Data access boundaries considered
- Cost drivers are understood

## Portfolio
- Major choices have rationale
- Evidence exists
- Limitations are honest
- User can explain architecture without memorizing vendor buzzwords
