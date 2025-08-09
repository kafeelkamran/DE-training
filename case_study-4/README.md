# Insurance Data Ingestion & Medallion Architecture Pipeline (ADF + Databricks)

##  Overview
This project implements a **config-driven ingestion pipeline** in **Azure Data Factory (ADF)** and **Azure Databricks** to process **3 domain tables** from source systems into an **Azure Data Lake Storage Gen2** container, following the **Medallion architecture** (Bronze → Silver → Gold).

The pipeline is designed to:
- Dynamically read configuration from a control table.
- Support **full** and **incremental** loads.
- Organize data in **Bronze, Silver, and Gold** layers for structured analytics.
- Use **SAS token authentication** for secure access to ADLS Gen2.
- Run on a scheduled trigger for automated ingestion.

---

##  Architecture

### **1. Control Table**
A metadata control table stores metadata for all 3 tables, including:
- **Source** (schema, table)
- **Ingestion type** (Full / Incremental)
- **Target sink path** in Data Lake
- **Status, retry count, and notes**

This allows **uniform ingestion** without hardcoding table details in the pipeline.

---

### **2. Azure Data Factory Pipeline**
The ADF pipeline contains:
1. **Lookup Activity**  
   - Reads eligible records (`Pending` or `Failed`) from the control table.
2. **ForEach Activity**  
   - Iterates over each table configuration.
3. **Data Flow / Copy Activity**  
   - For full loads: copies all data from the source.
   - For incremental loads: filters rows using the last processed watermark.
4. **Post-Processing**  
   - Updates the control table with the new watermark, run status, and timestamp.

---

### **3. Medallion Architecture**
We use **Azure Data Lake Storage Gen2** with three structured layers:

#### **Bronze Layer**
- Raw ingested data stored exactly as received.
- Minimal transformation.
- Used as the immutable source of truth.
- Example Path:

<img width="1365" height="764" alt="image" src="https://github.com/user-attachments/assets/ec6a36ee-fe23-4133-8d30-20b9db2887fa" />
