# Business Glossary

Plain-language definitions for non-engineering stakeholders.

| Term | Definition |
|---|---|
| **Lakehouse** | A single data platform that combines the flexibility of a data lake (raw, any format) with the reliability and structure of a data warehouse (reportable, trustworthy). |
| **Medallion Architecture** | The three-stage process data moves through: Bronze (raw, as received), Silver (cleaned and deduplicated), Gold (business-ready summaries and reports). |
| **Bronze Layer** | An exact, unaltered copy of the data exactly as it arrived from source systems — kept as a safety net so nothing is ever permanently lost or altered. |
| **Silver Layer** | Data that has been cleaned, deduplicated, and checked for quality — this is where "the true version" of each record lives. |
| **Gold Layer** | Business-ready summaries and reports built from Silver data — this is what powers dashboards and business decisions. |
| **Data Quality Validation** | Automated checks that catch bad data (missing values, impossible numbers, etc.) before it reaches business reports, rather than letting it silently produce wrong numbers. |
| **Quarantine** | Where records that fail a quality check are set aside — not deleted, not blocking the rest of the batch — so they can be reviewed and the upstream issue fixed. |
| **Historical Tracking / SCD Type 2** | The platform's ability to answer "what was true about this customer/product at the time of this specific sale" — not just "what's true about them today." Essential for accurate historical reporting. |
| **Incremental Loading** | Only processing new or changed data each day, rather than reprocessing everything from scratch — makes daily updates fast and efficient. |
| **Point-in-Time Correctness** | Ensuring reports reflect what was actually true at the time an event happened (e.g., a customer's location when they made a purchase), not what's true right now. |
| **Fact Table** | A table recording business events — sales, returns, shipments. Each row is typically one transaction or measurement. |
| **Dimension Table** | A table describing the "who/what/where" behind facts — customers, products, stores. |
| **Batch Processing** | Data is processed in scheduled runs (e.g., once daily) rather than instantly as it happens. |
| **Orchestration** | The automated scheduling and sequencing of data processing steps, including retry-on-failure and dependency management, so the platform runs unattended. |
| **Pass Rate** | The percentage of incoming records that pass all quality checks — a key indicator of upstream data health. |
