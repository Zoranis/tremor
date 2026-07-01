# Analyst SQL Query Pack

These queries answer the six defense questions from BRIEF.md.

## Files
- protest_hotspots.sql
- bilateral_trend.sql
- theme_surge.sql
- outlet_amplification.sql
- geographic_overlay.sql
- pipeline_self_audit.sql

## Notes
- `bilateral_trend.sql` is parameterized inline by actor/target country values.
- `theme_surge.sql` uses mentions joined to article themes by `slice_ts` because mentions rows do not include `primary_theme` directly.
- `pipeline_self_audit.sql` depends on observability tables/views in `src/schema.sql`.
