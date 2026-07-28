# Data Dictionary

## Dimension Tables

### `dim_customer` (SCD Type 2)
| Column | Type | Description |
|---|---|---|
| `customer_sk` | int | Surrogate key (PK) — unique per historical version |
| `customer_id` | string | Natural/business key from source system |
| `country` | string | Tracked column — triggers new historical version on change |
| `segment` | string | Tracked column |
| `effective_start_date` | date | Date this version became current |
| `effective_end_date` | date | Date this version stopped being current (NULL if still current) |
| `is_current` | boolean | True for the currently-active version of this customer |

**Why SCD2:** A customer's country/segment affects historical sales attribution; Gold-layer joins use the version that was true at transaction time, not today's value.

### `dim_product` (SCD Type 2)
| Column | Type | Description |
|---|---|---|
| `product_sk` | int | Surrogate key (PK) |
| `stock_code` | string | Natural key |
| `description` | string | Product name/description |
| `unit_price` | double | Tracked column — price changes trigger new version |
| `is_current` | boolean | Current version flag |

### `dim_store` (SCD Type 1)
| Column | Type | Description |
|---|---|---|
| `store_sk` | int | Surrogate key (PK) |
| `store_id` | string | Natural key |
| `region` | string | Store's operating region |
| `country` | string | Store's country |

**Why SCD1:** Store attribute changes are rare and historical accuracy of store metadata isn't business-critical — overwrite-in-place is a deliberate, defensible simplification.

### `dim_supplier` (SCD Type 1)
| Column | Type | Description |
|---|---|---|
| `supplier_sk` | int | Surrogate key (PK) |
| `supplier_id` | string | Natural key |
| `supplier_name` | string | — |

### `dim_date`
| Column | Type | Description |
|---|---|---|
| `date_sk` | int | `YYYYMMDD` format — enables fast partition pruning and joins |
| `full_date` | date | — |
| `year`, `month` | int | — |

## Fact Tables

### `fact_sales`
**Grain:** one row per (invoice_id, stock_code) — transaction line item

| Column | Type | Description |
|---|---|---|
| `invoice_id` | string | Degenerate dimension — kept on the fact, no separate dimension table |
| `customer_sk` | int (FK) | Resolved via point-in-time join to `dim_customer` |
| `product_sk` | int (FK) | Resolved via point-in-time join to `dim_product` |
| `store_sk` | int (FK) | Resolved via current-value join to `dim_store` (SCD1) |
| `sale_date` | date | Event date — drives the point-in-time dimension join |
| `quantity` | int | — |
| `unit_price` | double | Price at time of sale |
| `line_total` | double | Computed in Gold: `quantity * unit_price` |

### `fact_returns`
**Grain:** one row per returned line item

| Column | Type | Description |
|---|---|---|
| `return_invoice_id` | string | PK |
| `original_invoice_id` | string (FK) | Links to `fact_sales` for return-rate analysis |
| `product_sk` | int (FK) | — |
| `quantity` | int | Quantity returned |

### `fact_inventory_snapshot` (Periodic Snapshot Fact)
**Grain:** one row per (store_sk, product_sk, snapshot_date)

| Column | Type | Description |
|---|---|---|
| `store_sk` | int (FK) | — |
| `product_sk` | int (FK) | — |
| `snapshot_date` | date | Partition column |
| `stock_on_hand` | int | Point-in-time stock level |

**Why a periodic snapshot fact, not a transactional fact:** Inventory isn't a discrete event like a sale — daily point-in-time levels are needed for reorder analytics, the standard Kimball pattern for this use case.

### `fact_online_orders`
**Grain:** one row per order line item — same structure as `fact_sales` plus `order_status`, `channel`

### `fact_shipping`
**Grain:** one row per shipment

| Column | Type | Description |
|---|---|---|
| `shipment_id` | string | PK |
| `order_id` | string (FK) | Links to `fact_online_orders` |
| `carrier` | string | — |
| `ship_date` | date | — |

## Bridge Tables

### `bridge_promotions`
Many-to-many relationship between products and time-bound promotion rules.

| Column | Type | Description |
|---|---|---|
| `promo_id` | string | PK |
| `product_sk` | int (FK) | — |
| `start_date`, `end_date` | date | Promotion validity range |
| `discount_pct` | double | — |

## Metadata Columns (present on all Bronze and Silver tables)

| Column | Description |
|---|---|
| `_source_file` | Path of the raw file this row was ingested from |
| `_ingest_ts` | UTC timestamp of ingestion |
| `_batch_id` | UUID identifying the ingestion run |
| `_ingest_date` | Date partition column; also used as the Silver incremental watermark |

## Partitioning Summary

| Table | Partition Column | Rationale |
|---|---|---|
| Bronze (all tables) | `_ingest_date` | Enables reprocessing a single day without touching others |
| `fact_sales`, `fact_online_orders` (Silver/Gold) | `sale_date` / order date | Matches the dominant date-filtered query pattern |
| `fact_inventory_snapshot` | `snapshot_date` | Same reasoning; also this project's largest table (~29M rows at portfolio scale) |
| Dimension tables | Not partitioned by date | Small relative to facts; date-partitioning small tables causes the "small files problem" |

## Gold-Layer Z-Ordering

`fact_sales` is Z-ordered on `(customer_sk, product_sk)` — the two highest-cardinality, most-frequently-filtered join keys in typical BI queries against this mart. Low-cardinality columns (e.g. `store_sk`, ~20 distinct values) are deliberately excluded from Z-ordering, since the optimization cost isn't justified by the query benefit at that cardinality.
