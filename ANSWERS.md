# Lab 12 — Answers

## Part B — Run the pipeline by hand

### 1. pandas (Practice 10) → SQL in `dbt/models/staging/stg_sales.sql`

| pandas (Practice 10) | SQL in `stg_sales.sql` |
|---|---|
| `pd.to_datetime(..., errors="coerce")` | `try_cast(trim(order_date) as date) as order_date` — an unreadable date becomes `NULL` instead of crashing |
| `pd.to_numeric(...).fillna(0)` | `coalesce(try_cast(trim(quantity) as integer), 0) as quantity` |
| `drop_duplicates(subset="order_id")` | `qualify row_number() over (partition by order_id order by _source_file desc) = 1` |
| `df[df.quantity >= 0]` (drop negative quantity) | `where quantity >= 0` |
| `total = quantity * price` | `quantity * price as total_amount` |

### 2. `raw.sales` has 244 rows, `dw.fact_sales` has 241. Where did 3 rows go?

Each row is removed by a different cleaning rule:

| Step | Rows | Removed row | Rule |
|---|---|---|---|
| `raw.sales` | 244 | | |
| after de-duplication | 243 | `order_id 1011` appears twice | `qualify row_number() ... = 1` in `stg_sales` |
| after negative filter → `staging.stg_sales` | 242 | `order_id 1241`, `quantity = -1` | `where quantity >= 0` in `stg_sales` |
| `dw.fact_sales` | 241 | `order_id 1242`, `order_date = 'not-a-date'` | `where order_date is not null` in `fact_sales` |

Proof:

```bash
make sql Q="SELECT count(*), count(DISTINCT order_id) FROM raw.sales"          # 244, 243
make sql Q="SELECT count(*) FROM staging.stg_sales"                            # 242
make sql Q="SELECT count(*) FROM dw.fact_sales"                                # 241
make sql Q="SELECT order_id, count(*) FROM raw.sales GROUP BY 1 HAVING count(*) > 1"   # 1011
make sql Q="SELECT * FROM raw.sales WHERE try_cast(trim(quantity) AS int) < 0"         # 1241
make sql Q="SELECT * FROM staging.stg_sales WHERE order_date IS NULL"                  # 1242
```

(These counts are for the original `sales_2024_h1.csv` only. After `make new-day`
the July file adds 45 more rows.)

### 3. Which test gives the warning, and why is it a warning and not an error?

The warning is `not_null_stg_sales_order_date`. It finds 1 row: order 1242, whose
date `not-a-date` could not be converted.

In `dbt/models/staging/schema.yml` the test is configured with
`severity: warn`. The bad row is a known, handled problem: it is kept in
staging so we can see it, and `fact_sales` filters it out with
`where order_date is not null`. So the warehouse is still correct and there
is no reason to stop the pipeline. The warning just tells us bad data arrived.
An error (the default severity) would make `dbt test` fail and stop the DAG.

### 4. How does dbt know `stg_sales` must be built before `fact_sales`?

Because of `{{ ref('stg_sales') }}`. Every `ref()` (and `source()`) tells dbt
"this model depends on that one". dbt reads all the `ref()` calls, builds a
dependency graph (a DAG, the same idea as in Airflow) and runs the models in
the right order: `raw` → `stg_*` → `dim_*` / `fact_sales` → `mart_*`. The
lineage graph in `make docs` shows this graph.

---

## Part C — Let Airflow run it

The `retail_pipeline` DAG ran (`make run-dag`, or Trigger in the UI); all 5 tasks finished
green: `check_source_files → extract_load → dbt_run → dbt_test → report`.

- **Report log (step 3):** the `report` task prints the monthly revenue per
  category from `dw.mart_monthly_revenue`, e.g. `2024-01 | Electronics | 59,950,000`.
- **XCom (step 4):** `extract_load` returns the row counts it loaded:
  `{"customers": 20, "products": 10, "sales": 244}`.
- **Schedule (step 5):** `schedule="@daily"` (cron `0 0 * * *`, once a day at
  midnight), with `catchup=False` so Airflow does not re-run every missed day
  since `start_date`.
- **Retries (step 5):** `default_args` gives every task `retries: 1` with
  `retry_delay` of 20 seconds. `dbt_test` overrides this with `retries=0`,
  because a failed data test will not fix itself.
- **Task order (step 5):** `loaded >> dbt_run >> dbt_test >> report()`. The
  `>>` operator sets the dependencies; `extract_load(check_source_files())`
  links the first two tasks by passing the file list between them (TaskFlow).
  After Part E the line ends `... >> report() >> export_csv()`.

---

## Part D — Automation in action

### Scenario 1 — new data arrives

After `make new-day` and triggering the DAG again, **2024-07** appears in the
report. No code was changed: `extract_load.py` loads every `sales_*.csv` file it
finds in `data/raw/`, so the new file was picked up by itself.

### Scenario 2 — bad data arrives

- **Which task turns red?** `dbt_test`.
- **Which test fails?**
  `relationships_fact_sales_customer_id__customer_id__ref_dim_customer_`.
  The log says `Got 1 result, configured to fail if != 0`: order 1289 has
  `customer_id = 999`, which does not exist in `dim_customer` (an orphan row).
- **Which task did not run, and why is that good?** `report` did not run (state
  `upstream_failed`). Airflow only runs a task when the task before it succeeded,
  so nobody gets a report built from broken data. Also, `dbt_test` has
  `retries=0`, because retrying a data test will not fix the data.

After `make fix` and triggering again, all tasks are green.

### Discussion — was the bad row already in `dw.fact_sales`?

**Yes.** After the failed run:

```bash
make sql Q="SELECT * FROM dw.fact_sales WHERE customer_id = 999"
# order_id 1289 | 2024-08-03 | customer_id 999 | ...
```

`dbt run` builds *all* the tables first and `dbt test` only checks them
afterwards, so the test finds the problem after the bad row is already in the
warehouse.

**How to stop bad data before it reaches the warehouse:**

- Use **`dbt build`** instead of `dbt run` + `dbt test`. `dbt build` runs and
  tests each model in dependency order. If a test on a model fails, the models
  that depend on it are **skipped**, so the bad data does not flow further.
  In the DAG, `dbt_run >> dbt_test` would become one `dbt build` task.
- Put the check **earlier**, e.g. a `relationships` test on
  `stg_sales.customer_id` → `stg_customers.customer_id`. Then `dbt build`
  stops at staging and `fact_sales` is never rebuilt with the orphan row.

---

## Part E — Exercises

| # | What | File |
|---|---|---|
| 1 | Sales per region per month (join `dim_customer`) | `dbt/models/marts/mart_sales_region_month.sql` |
| 2 | Top 5 products per category (`ROW_NUMBER()` + `QUALIFY`) | `dbt/models/marts/mart_top5_products_per_category.sql` |
| 3 | Month-over-month revenue growth (`LAG()`) | `dbt/models/marts/mart_revenue_growth.sql` |
| 4 | Customers with the highest total spending (`RANK()`) | `dbt/models/marts/mart_top_customers.sql` |
| 5 | New tests: `accepted_values` on `dim_product.category`; ranks 1–5 in the top-5 mart; `unique` + `not_null` on `mart_revenue_growth.ym` and `mart_top_customers.customer_id` | `dbt/models/marts/schema.yml` |
| 6 | New DAG task `export_csv` after `report`: writes `dw.mart_monthly_revenue` to `data/export/monthly_revenue.csv` and returns the file path as an XCom | `dags/retail_pipeline.py` |

The DAG is now:

```
check_source_files >> extract_load >> dbt_run >> dbt_test >> report >> export_csv
```

### Results

- `make transform`: `PASS=12 WARN=0 ERROR=0` (12 models)
- `make test`: `PASS=24 WARN=1 ERROR=0` (the 1 warning is the expected
  `order_date` warning from Part B, question 3)
- `make run-dag`: all 6 tasks `success`; `export_csv` log:
  `Wrote 62 rows to data/export/monthly_revenue.csv` (62 rows with the July file
  from `make new-day`; 53 rows with only the January–June data)

Example output, `mart_revenue_growth`:

| ym | revenue | prev_revenue | growth_pct |
|---|---|---|---|
| 2024-01 | 78,750,000 | — | — |
| 2024-02 | 181,150,000 | 78,750,000 | 130.03 |
| 2024-03 | 131,650,000 | 181,150,000 | -27.33 |
| 2024-04 | 83,950,000 | 131,650,000 | -36.23 |
| 2024-05 | 109,550,000 | 83,950,000 | 30.49 |
| 2024-06 | 130,550,000 | 109,550,000 | 19.17 |

Example output, `mart_top_customers` (top 3):

| rank | customer | region | orders | total spent |
|---|---|---|---|---|
| 1 | Nanda Pratama | Papua | 19 | 65,900,000 |
| 2 | Budi Santoso | Kalimantan | 17 | 64,200,000 |
| 3 | Sinta Dewi | Sulawesi | 21 | 57,550,000 |
