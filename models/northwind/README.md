  - What business problem does your dbt model solve?

The dbt project solves three problems Northwind had: inconsistent column naming, repeated manual joins across analysts, and different definitions of revenue. By building a staging → prep → mart pipeline, we standardized the data once and created a single source of truth that any analyst or BI tool can query.

  - Which models did you build, and what does each do?

* Staging layer — cleans the raw data:

staging_orders.sql — renames columns, casts date fields
staging_order_details.sql — cleans line item data, casts price and quantity types
staging_products.sql — standardizes product fields, keeps only sales-relevant columns
staging_categories.sql — pulls category id and name only

* Prep layer — joins and enriches:

prep_sales.sql — joins all 4 staging models together, calculates revenue per line item, extracts year and month

* Mart layer — aggregates for analysis:

mart_sales_performance.sql — aggregates revenue, order count and avg revenue per order by category, month and year

  - What insights can your mart provide to Northwind?

The mart allows Northwind to track revenue trends by product category over time. Key questions it can answer: which categories are growing, which are volatile (e.g. Meat/Poultry), and where unexpected spikes occurred (e.g. Beverages and Dairy in early 1998). It could also be used to spot seasonality patterns and prioritize categories for commercial decisions.

- What was your biggest learning moment in this project?

The biggest learning moment was building the full pipeline end to end — configuring the sources in the yml file, writing staging, prep and mart models from scratch, and seeing the tables actually appear in the database after running dbt. The testing yml was also a highlight — understanding that dbt auto-generates SQL checks behind the scenes made data validation feel much less intimidating.