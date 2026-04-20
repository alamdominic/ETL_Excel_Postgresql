# Excel ETL Pipeline - Presupuesto SAD

[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **Automated ETL pipeline for extracting, transforming, and loading budget data from Excel files into PostgreSQL database with intelligent synchronization and email notifications.**

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Synchronization Mechanism](#synchronization-mechanism)
- [Project Structure](#project-structure)
- [Logging](#logging)
- [Email Notifications](#email-notifications)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

This ETL (Extract, Transform, Load) pipeline automates the process of importing budget data from Excel files into a PostgreSQL database. It includes advanced features like intelligent synchronization to avoid duplicate records, data validation, comprehensive logging, and automated email notifications.

### Key Capabilities

- **Excel Extraction**: Reads data from Excel files using `openpyxl`
- **Data Transformation**: Converts, cleans, and normalizes data using `pandas`
- **Smart Synchronization**: Compares database and Excel IDs to insert only new records
- **Data Validation**: Validates data types, ranges, and business rules
- **Database Loading**: Efficient bulk insert into PostgreSQL using `psycopg2`
- **Logging**: Dual-mode logging (console + rotating files)
- **Email Notifications**: Automated success/failure notifications via SMTP

---

## ✨ Features

### 🔄 Intelligent Synchronization

- Compares the last ID in the database with the last ID in Excel
- **Full Load**: Inserts all records when database is empty
- **Incremental Load**: Inserts only new records (id > last_db_id)
- **Skip Load**: No action when already synchronized
- **Error Detection**: Alerts on data integrity issues

### 📊 Data Quality

- Column type validation
- Business rule enforcement
- Duplicate detection
- Date format normalization
- Null value handling

### 📧 Automated Notifications

- HTML-formatted success emails with statistics
- Detailed failure notifications with error context
- Configurable recipients and SMTP settings

### 📝 Comprehensive Logging

- Console output for real-time monitoring
- Rotating file logs (10MB max, 5 backups)
- Stage-by-stage execution tracking
- Error traceability with full context

---

## 🏗️ Architecture

The project follows a **clean layered architecture** with strict separation of concerns:

```
┌─────────────────────────────────────────────────────────┐
│             ORCHESTRATION LAYER                         │
│              (app/orchestrator.py)                      │
└────────────────────────┬────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ↓               ↓               ↓
    ┌─────────┐     ┌─────────┐    ┌─────────┐
    │ EXTRACT │     │TRANSFORM│    │  LOAD   │
    │  LAYER  │     │  LAYER  │    │  LAYER  │
    └────┬────┘     └────┬────┘    └────┬────┘
         │               │               │
         └───────────────┼───────────────┘
                         ↓
         ┌───────────────────────────────┐
         │    REPOSITORY LAYER           │
         │     (Data Access)             │
         └───────────────┬───────────────┘
                         ↓
         ┌───────────────────────────────┐
         │   CONFIGURATION LAYER         │
         │     (db, excel)               │
         └───────────────────────────────┘
```

**Layer Responsibilities:**

- **Orchestration**: Coordinates pipeline execution flow
- **Extraction**: Reads Excel files (openpyxl)
- **Transformation**: Data cleaning and normalization (pandas)
- **Loading**: Bulk insert to PostgreSQL
- **Repository**: Database access abstraction
- **Configuration**: Environment variables and settings

### Pipeline Stages

1. **Configuration**: Load environment variables and database connections
2. **Extraction**: Read Excel file and sheet
3. **Transformation**: Convert to pandas DataFrame
4. **Normalization**: Validate and clean data
5. **Synchronization**: Compare IDs and determine sync strategy
6. **Loading**: Bulk insert into PostgreSQL
7. **Notification**: Send email with execution report

---

## 📦 Requirements

### System Requirements

- **Python**: 3.9 or higher
- **PostgreSQL**: 9.6 or higher
- **Operating System**: Windows, Linux, or macOS

### Python Dependencies

```
pandas>=2.0.0
numpy>=1.26.0
openpyxl>=3.1.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
python-dotenv>=1.0.0
python-dateutil>=2.8.2
```

See [requirements.txt](requirements.txt) for complete list.

---

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/etl-sad.git
cd etl-sad
```

### 2. Create Virtual Environment

**Windows:**

```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/macOS:**

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example configuration file:

```bash
cp .env.example .env
```

Edit `.env` with your credentials (see [Configuration](#configuration) section).

---

## ⚙️ Configuration

### Database Configuration

Edit `.env` file with your PostgreSQL credentials:

```env
# PostgreSQL Database
DB_HOST="your-database-host.com"
DB_NAME="your_database_name"
DB_USER="your_username"
DB_PASSWORD="your_password"
DB_PORT="5432"

# Target Schema and Table
SAD_SCHEMA="your_schema"
SAD_TABLE="your_table"
```

### Excel Configuration

```env
# Excel File Path and Sheet
SAD_EXCEL_PATH="path/to/your/excel/file.xlsx"
SAD_SHEET="Sheet1"
```

### Email Configuration (Optional)

For **Microsoft Outlook/Office 365**:

1. Use your Outlook/Office 365 email credentials
2. If using 2FA, generate an App Password: https://account.live.com/proofs/AppPassword
3. Configure:

```env
EMAIL_SENDER="your.email@outlook.com"
EMAIL_PASSWORD="your-app-password"
EMAIL_RECIPIENT="recipient@company.com"
SMTP_HOST="smtp-mail.outlook.com"
SMTP_PORT="587"
```

For **Other Providers**:

- Gmail: `smtp.gmail.com:587` (requires App Password)
- Yahoo: `smtp.mail.yahoo.com:587`
- Office 365 (work/school): `smtp.office365.com:587`

### Logging Configuration (Optional)

```env
LOG_LEVEL="INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

---

## 🎮 Usage

### Basic Execution

Run the ETL pipeline:

```bash
python app.py
```

### Expected Output

```
================================================================================
ETL PIPELINE EXECUTION STARTED
================================================================================
Start time: 2026-04-20 10:30:15

STAGE 1: Loading configuration...
  ✓ Excel source: C:\Data\budget.xlsx → Sheet: Sheet1
  ✓ Target table: "DataMart"."presupuesto_sad"

STAGE 2: Extracting data from Excel...
  ✓ Worksheet loaded: 1550 rows × 3 columns

STAGE 3: Converting to DataFrame...
  ✓ DataFrame created: 1549 rows × 3 columns
  ✓ Columns: ['id', 'Fecha', 'Rebanadas']

STAGE 4: Normalizing and validating data...
  ✓ Original rows: 1549
  ✓ Rows after normalization: 1549
  ✓ Rows removed: 0
  ✓ Data validation passed

STAGE 5: Checking synchronization status...
  ✓ Last id in database: 1500
  ✓ Last id in Excel: 1549
  → New records detected: 49 records with id > 1500
  ✓ Synchronization mode: incremental_load
  ✓ Records to insert: 49

STAGE 6: Loading data into PostgreSQL...
  → Inserting 49 rows into "DataMart"."presupuesto_sad"...
  ✓ Successfully inserted 49 rows

================================================================================
ETL PIPELINE COMPLETED SUCCESSFULLY
  → Synchronization mode: incremental_load
  → Rows inserted: 49
  → Execution time: 8.32 seconds
================================================================================
```

### Testing Synchronization Logic

Run the synchronization test script:

```bash
python test_sync.py
```

### View Synchronization Diagram

Display visual representation of sync mechanism:

```bash
python sync_diagram.py
```

---

## 🔄 Synchronization Mechanism

The pipeline includes an **intelligent synchronization system** that prevents duplicate records:

### How It Works

1. **Get last ID from database**: `SELECT MAX("id") FROM table`
2. **Get last ID from Excel**: `df["id"].max()`
3. **Compare and decide**:

| Condition       | Action             | Example                                                     |
| --------------- | ------------------ | ----------------------------------------------------------- |
| **DB is empty** | Insert ALL records | DB: `None` → Excel: `2000` → Insert: **2000**               |
| **Excel > DB**  | Insert ONLY new    | DB: `1500` → Excel: `1550` → Insert: **50** (IDs 1501-1550) |
| **Excel == DB** | Skip (no changes)  | DB: `1550` → Excel: `1550` → Insert: **0**                  |
| **Excel < DB**  | Error (integrity)  | DB: `1500` → Excel: `1400` → **ABORT**                      |

### Benefits

✅ **No Duplicates**: Only inserts records with `id > last_db_id`  
✅ **Performance**: Processes only new records  
✅ **Data Integrity**: Detects inconsistencies  
✅ **Audit Trail**: Logs every sync decision

See [SYNC_MECHANISM.md](SYNC_MECHANISM.md) for detailed documentation.

---

## 📁 Project Structure

```
etl-sad/
├── app/
│   ├── config/               # Configuration modules
│   │   ├── db_config.py      # Database connection
│   │   ├── excel_config.py   # Excel file settings
│   │   └── logging_config.py # Logging configuration
│   ├── error/                # Custom exceptions
│   │   └── exceptions.py
│   ├── extract/              # Extraction layer
│   │   └── excel_extractor.py
│   ├── transform/            # Transformation layer
│   │   ├── excel_to_dataframe_converter.py
│   │   └── data_normalizer.py
│   ├── load/                 # Loading layer (legacy)
│   │   └── etl_repository.py
│   ├── repository/           # Data access layer
│   │   ├── i_repository.py   # Interface
│   │   ├── base_repository.py
│   │   └── postgresql_repository.py
│   ├── utils/                # Utility functions
│   │   └── email_notifier.py # Email notifications
│   ├── sql/                  # SQL queries (if needed)
│   └── orchestrator.py       # Main pipeline orchestrator
├── logs/                     # Log files (auto-generated)
├── app.py                    # Entry point
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
├── .gitignore                # Git ignore rules
├── README.md                 # This file
├── SYNC_MECHANISM.md         # Sync documentation
└── test_sync.py              # Synchronization tests
```

---

## 📊 Logging

### Console Logging

Real-time output with color-coded log levels (if supported by terminal).

### File Logging

- **Location**: `logs/etl_pipeline_YYYYMMDD.log`
- **Rotation**: 10MB max per file, 5 backups
- **Format**: `YYYY-MM-DD HH:MM:SS - module_name - LEVEL - message`

### Example Log Entry

```
2026-04-20 10:30:15 - app.orchestrator - INFO - STAGE 5: Checking synchronization status...
2026-04-20 10:30:15 - app.repository.postgresql_repository - INFO - Last id in "DataMart"."presupuesto_sad": 1500
2026-04-20 10:30:15 - app.orchestrator - INFO -   ✓ Last id in database: 1500
2026-04-20 10:30:15 - app.orchestrator - INFO -   ✓ Last id in Excel: 1549
2026-04-20 10:30:15 - app.orchestrator - INFO -   → New records detected: 49 records with id > 1500
```

---

## 📧 Email Notifications

### Success Notification

**Subject**: `ETL Pipeline - Execution Successful`

**Content**:

```
ETL Pipeline executed successfully.

Summary:
- Rows inserted: 49
- Execution time: 8.32 seconds
- Status: SUCCESS

No action required.
```

### Failure Notification

**Subject**: `ETL Pipeline - Execution Failed (Stage Name)`

**Content**:

```
ETL Pipeline execution FAILED.

Failure Stage: Synchronization
Error Message: Data integrity error: Excel last id (1400) is lower than database last id (1500)

Action Required:
- Review pipeline logs for detailed error trace
- Verify data source availability
- Check database connectivity and credentials

Timestamp: 2026-04-20 10:30:45
```

---

## 🧪 Testing

### Manual Tests

1. **First Load (Empty Database)**

   ```bash
   # Clear target table, then run pipeline
   python app.py
   # Expected: All records inserted
   ```

2. **Incremental Load**

   ```bash
   # Add new rows to Excel, then run pipeline
   python app.py
   # Expected: Only new records inserted
   ```

3. **Already Synchronized**
   ```bash
   # Run pipeline without changing Excel
   python app.py
   # Expected: 0 records inserted
   ```

### Automated Tests

Run synchronization logic tests:

```bash
python test_sync.py
```

---

## 🔧 Troubleshooting

### Problem: "Column 'id' not found"

**Cause**: Excel file missing `id` column  
**Solution**: Ensure Excel has columns: `id`, `Fecha`, `Rebanadas`

### Problem: "Data integrity error"

**Cause**: Excel has lower max ID than database  
**Solution**: Verify Excel file is up-to-date

### Problem: Email not sending

**Cause**: SMTP credentials invalid  
**Solution**:

1. For Outlook/Office 365, use App Password if 2FA is enabled
2. Verify SMTP host (`smtp-mail.outlook.com`) and port (587) settings
3. Check firewall/antivirus blocking port 587
4. For Gmail, use App Password: https://myaccount.google.com/apppasswords

### Problem: Database connection timeout

**Cause**: Incorrect credentials or network issue  
**Solution**:

1. Verify DB_HOST, DB_USER, DB_PASSWORD in `.env`
2. Test database connectivity: `psql -h HOST -U USER -d DB_NAME`
3. Check firewall allows PostgreSQL port (default: 5432)

### Problem: Performance issues with large files

**Solution**:

1. Increase batch size in `bulk_insert` (currently 1000)
2. Consider chunking for files > 100MB
3. Add database indexes on `id` column

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork** the repository
2. Create a **feature branch**: `git checkout -b feature/AmazingFeature`
3. **Commit** changes: `git commit -m 'Add some AmazingFeature'`
4. **Push** to branch: `git push origin feature/AmazingFeature`
5. Open a **Pull Request**

### Code Style

- Follow PEP 8 guidelines
- Use type hints where applicable
- Document functions with docstrings
- Add unit tests for new features

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 📚 Additional Documentation

- **[SYNC_MECHANISM.md](SYNC_MECHANISM.md)** - Detailed synchronization documentation
- **[SYNC_IMPLEMENTATION_SUMMARY.md](SYNC_IMPLEMENTATION_SUMMARY.md)** - Implementation summary
- **[ARCHITECTURE_REVIEW.md](ARCHITECTURE_REVIEW.md)** - Architecture overview

---

## 🆘 Support

For issues, questions, or feature requests:

1. Check [Troubleshooting](#troubleshooting) section
2. Review logs in `logs/etl_pipeline_YYYYMMDD.log`
3. Open an issue on GitHub
4. Contact: [your-email@example.com](mailto:your-email@example.com)

---

## 🙏 Acknowledgments

- Built with Python 3.13+
- Uses pandas for data transformation
- PostgreSQL for data warehouse
- Developed following clean architecture principles

---

**Made with ❤️ for efficient data pipeline automation**
