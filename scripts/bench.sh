#!/usr/bin/env bash
set -euo pipefail
DB="${DB:-postgresql:///olist}"
mkdir -p docs
echo "Benchmarking..."
psql "$DB" -X -t -A -f sql/02_queries.sql >/dev/null 2>&1 || true

# Core timing sample
q1="EXPLAIN ANALYZE SELECT SUM(price+freight_value)
    FROM dw.fact_order_item
    WHERE date_key_purchase BETWEEN 20170101 AND 20170131;"
echo "$q1" | psql "$DB" -X -t -A | tee docs/bench_q1.txt



