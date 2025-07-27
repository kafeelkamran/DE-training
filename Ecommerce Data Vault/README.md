# 🛒 Ecommerce Data Vault & Star Schema Project

A comprehensive end-to-end data engineering project demonstrating **Data Vault 2.0 modeling**, **Delta Lake ingestion**, **Slowly Changing Dimensions (SCD)**, and **Star Schema** views for analytics. This project simulates a real-world ecommerce dataset pipeline with proper SCD handling, schema evolution, GDPR logic, and analytical view creation.

---

## 📁 Project Structure

/ecommerce_data_vault/
├── er_diagrams/                    # ER diagrams and SQL modeling for raw vault & star schema
│   ├── raw_vault_erd.png          # Visual ER diagram for Raw Data Vault
│   ├── raw_vault_erdmodel.sql     # DBML/SQL schema for Raw Vault (used in dbdiagram.io)
│   ├── star_schema_erdmodel.sql   # DBML/SQL schema for Star Schema (used in dbdiagram.io)
│   └── star_schema_erd.png        # Visual ER diagram for Star Schema

├── delta_tables/                  # DDL scripts and lifecycle management of delta tables
│   ├── create_tables.sql          # SQL to create all required delta tables
│   ├── schema_evolution.sql       # Script to handle schema evolution in delta tables
│   └── gdpr_deletion.sql          # Script for GDPR-compliant data deletion logic

├── scd_scripts/                   # Slowly Changing Dimension (SCD) and PIT/Bridge logic
│   ├── scd_type_2_merge.sql       # Merge logic for implementing SCD Type-2
│   ├── pit_bridge.py              # Python code for generating PIT/Bridge tables
│   └── surrogate_key_macro.py     # Python macro or logic to generate surrogate keys

├── automation/                    # Data pipeline automation scripts
│   ├── load_pipeline.py           # ETL/ELT orchestration to load data into delta tables
│   └── hash_key_generator.py      # Utility to generate consistent hash keys for hubs

├── star_schema_views/            # Star schema view creation scripts and results
│   ├── output/star_views/        # Parquet output of star views
│   ├── salesfact_view.sql        # SQL view logic for sales fact star schema
│   ├── customerfact_view.sql     # SQL view logic for customer fact star schema
│   ├── inventoryfact_view.sql    # SQL view logic for inventory fact star schema
│   ├── run_salesfact.py          # Script to run salesfact_view.sql and save output
│   ├── builder_star_view.py      # Script to load all star views from SQL files
│   └── dummy_delta.py            # Script to populate dummy delta data for testing

├── docs/                          # Documentation related to modeling and standards
│   ├── grain_definitions.md      # Granularity of fact tables and hub/sat definitions
│   ├── SCD_checklist.md          # Checklist and best practices for implementing SCD
│   └── peer_review_notes.md      # Notes and checklist for peer review of data models

└── .git/                          # Git metadata folder for version control



---

## 🚀 Features

- ✅ **Delta Table Creation** using Delta Lake for raw vault and star schema
- ✅ **SCD Type 2 Implementation** for historical tracking
- ✅ **Hash Key-Based Hub Generation**
- ✅ **PIT & Bridge Table Creation**
- ✅ **Schema Evolution Handling**
- ✅ **GDPR Compliant Deletion Scripts**
- ✅ **Star Schema View Creation** from delta tables using SQL and PySpark
- ✅ **Automated View Builder** to materialize all views to Parquet

---

## 📊 Star Schema Views

Each star view is created using SQL (`*.sql`) and executed using Spark. Output is saved as Parquet.

- `salesfact_view.sql`: Sales Fact with product and customer dim joins
- `customerfact_view.sql`: Customer Fact with profile SAT data
- `inventoryfact_view.sql`: Inventory Fact with warehouse dimension

Use `builder_star_view.py` to run all view SQLs at once.

```bash
python builder_star_view.py
```

### ⚙️ Automation Scripts
    load_pipeline.py: Ingests raw data into Delta tables

    hash_key_generator.py: Creates consistent hash keys (e.g., for hub primary keys)

    dummy_delta.py: Loads dummy data for local testing

### 📂 Delta Table Scripts
    create_tables.sql: Full DDL for all hubs, sats, links, and facts

    schema_evolution.sql: Adds/updates columns to delta tables as per schema changes

    gdpr_deletion.sql: Ensures compliance for user data deletion on request

### 📚 Documentation
    grain_definitions.md: Defines granularity for all entities

    SCD_checklist.md: What to verify when implementing SCD

    peer_review_notes.md: QA checklist for reviewing models and views

### 📈 ER Diagrams
    raw_vault_erd.png: Raw Data Vault model

    star_schema_erd.png: Star schema view model

Use raw_vault_erdmodel.sql and star_schema_erdmodel.sql in dbdiagram.io

🧪 How to Run
Run dummy_delta.py to create test delta tables.

Run builder_star_view.py to create star views and save as Parquet.

View output at /star_schema_views/output/star_views/.

✅ Prerequisites
Python 3.10+

Apache Spark 3.x with Delta Lake enabled

PySpark

Delta Lake JARs (io.delta:delta-core_2.12:2.4.0)

Pandas (for automation scripts)

🔍 Future Enhancements
Add Link tables and bridge joins for complex facts

Add real ingestion pipelines using Kafka/Structured Streaming

Add Power BI/Looker dashboards on top of star views

👤 Author
Kafeel Kamran Ahmed

