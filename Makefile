DB?=postgresql:///olist
.PHONY: schema load kpis

schema: ; psql $(DB) -f sql/01_schema.sql
load:   ; python scripts/load.py
kpis:   ; psql $(DB) -c "REFRESH MATERIALIZED VIEW dw.mv_kpi_monthly; SELECT * FROM dw.mv_kpi_monthly ORDER BY year DESC, month DESC LIMIT 6;"
