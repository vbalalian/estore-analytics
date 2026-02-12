# Marts: Session Cleanup

## What it does

`fct_sessions_cleaned` replaces `fct_sessions` as the canonical session fact table. It applies a configurable inactivity timeout (default: 1 hour) to split sessions that span unreasonably long periods into shorter, realistic sessions.

When the gap between consecutive events within a session exceeds the timeout threshold, the model treats the next event as the start of a new session and assigns a new session ID (`original_id_timeout_N`). Sessions that were never split keep their original ID.

## Why it exists

2.16% of sessions in the raw data span up to 6 months with massive inactivity gaps. This is a data collection bug — session IDs were reused across separate browsing sessions without an inactivity timeout. These outlier sessions distort key metrics like average session length (inflated from ~215 seconds to ~40,098 seconds).

## How it works

1. Events are pulled from `fct_events` with only the columns needed for aggregation
2. `LAG()` computes the time gap between consecutive events within each original session
3. Gaps exceeding `var('session_timeout_seconds')` (default 3600s) are flagged as timeouts
4. A running `SUM()` of timeout flags creates sub-session groupings
5. New session IDs are generated for split sessions (`original_id_timeout_1`, `_timeout_2`, etc.)
6. Events are aggregated into cleaned sessions with the same output structure as `fct_sessions`

The 1-hour threshold is a conservative choice — Google Analytics uses 30 minutes. One hour avoids splitting legitimate long-browsing sessions while catching the multi-month gaps. The threshold is configurable via `session_timeout_seconds` in `dbt_project.yml`.

## Business impact

- ~2.16M sessions split or removed (~2.43% of total)
- Average session length drops from ~40,098s to ~215s (realistic)
- Minimal impact on conversion and revenue metrics (all changes <1%)
- Output is a drop-in replacement for `fct_sessions` (same column structure)

## How to run

```bash
dbt run --select fct_sessions_cleaned
dbt test --select fct_sessions_cleaned
dbt docs generate
```

## Future improvements

- Migrate downstream models (`metrics_conversion_rates`, `dim_users`, etc.) to reference `fct_sessions_cleaned`
- Consider incremental materialization for faster refreshes on the 400M+ event dataset
- Evaluate whether the timeout threshold should vary by user segment or device type
