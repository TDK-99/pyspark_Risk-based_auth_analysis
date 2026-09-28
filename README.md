# Risk-Based Authentication Analysis

PySpark analysis of **33M+ login attempts** from a large-scale SSO service, exploring patterns in authentication risk factors (IP geolocation, device type, RTT latency) using a distributed Spark cluster on Podman.

## Architecture

```
                   +------------------+
                   |   Spark Master   |
                   |   (port 7077)    |
                   +--------+---------+
                            |
          +---------+-------+-------+---------+
          |         |               |         |
     Worker 1  Worker 2       Worker 3  Worker 4
     (1c/1GB)  (1c/1GB)      (1c/1GB)  (1c/1GB)
```

The pipeline reads the [RBA Dataset](https://www.kaggle.com/datasets/dasgroup/rba-dataset) from S3, validates and analyzes it across 4 Spark workers, then exports the results as an Excel report uploaded to S3 and sent via email.

## Pipeline

```
main.py
  |
  |-- 1. builder()            Build SparkSession + import data from S3
  |                           (CSV -> Parquet conversion on first run)
  |
  |-- 2. data_validation()    7 validation checks on the DataFrame
  |
  |-- 3. analysis()           6 Spark SQL analytics queries
  |
  |-- 4. export_and_upload()  Excel export + S3 upload
  |
  |-- 5. send_email()         SMTP email with report attached
```

## Data Validation

| # | Check | Action |
|---|-------|--------|
| 1 | Schema type matching (16 columns) | Raise if mismatch |
| 2 | Not-null constraints on `index`, `User ID`, `IP Address`, `Login Timestamp` | Raise if null found |
| 3 | Max null % per column (`Region` < 0.15%, `City` < 0.10%, `Device Type` < 0.01%) | Raise if exceeded |
| 4 | Country codes are exactly 2 characters (ISO 3166-1 alpha-2) | Raise if invalid |
| 5 | Row count in expected range (30M - 60M) | Raise if out of range |
| 6 | No duplicate rows | Raise if duplicates found |
| 7 | Fill null `Device Type` values with `"unknown"` | Imputation |

## Analysis Queries

| Query | Description |
|-------|-------------|
| **Overview KPIs** | Total logins, success/fail, attack rate, takeover rate, distinct users/IPs |
| **Attacks by Hour** | Attack distribution across 24 hours |
| **Top Countries** | Top 20 countries by attack count (min 1000 logins) |
| **Device Profiling** | Attack rates segmented by device type |
| **Targeted Users** | Top 20 most-attacked users with geo spread |
| **RTT Comparison** | Round-trip time statistics: attack vs legitimate traffic (avg, median, p25/p75/p95) |

## Setup

### Prerequisites

- [Podman](https://podman.io/) or Docker with Compose
- AWS account with an S3 bucket (`sparkanalysisauth`)
- SMTP credentials (Gmail, Outlook, or other provider)

### 1. Clone and configure

```bash
git clone https://github.com/tdk-99/pyspark_risk-based_auth_analysis.git
cd pyspark_risk-based_auth_analysis

cp env.example .env
# Edit .env with your credentials
```

### 2. Get the dataset

Download the [RBA Dataset](https://www.kaggle.com/datasets/dasgroup/rba-dataset) and upload `rba-dataset.csv` to your S3 bucket:

```bash
aws s3 cp rba-dataset.csv s3://sparkanalysisauth/rba-dataset.csv
```

On first run, the pipeline converts it to Parquet for faster subsequent reads.

### 3. Start the cluster

```bash
podman-compose up -d
```

This spins up:

| Service | Description | Port |
|---------|-------------|------|
| `spark-master` | Apache Spark 4.2.0 master | 8080 (UI), 7077 (cluster) |
| `spark-worker-1..4` | 4 workers, 1 core / 1 GB each | - |
| `jupyter` | JupyterLab for exploration | 8888 (Lab), 4040 (Spark UI) |

### 4. Run the pipeline

```bash
podman exec -it <jupyter-container> python /app/main.py
```

## SMTP Configuration

The email module is provider-agnostic. Configure via `.env`:

| Provider | `SMTP_HOST` | `SMTP_PORT` |
|----------|-------------|-------------|
| Gmail | `smtp.gmail.com` | `465` (SSL) |
| Outlook / Office 365 | `smtp.office365.com` | `587` (STARTTLS) |

For Gmail, use an [App Password](https://support.google.com/accounts/answer/185833) (requires 2FA enabled).

## Project Structure

```
.
├── main.py                 # Pipeline orchestrator
├── src/
│   ├── __init__.py
│   ├── config_import.py    # SparkSession builder + S3 data import
│   ├── validation.py       # 7-step data validation
│   ├── analysis.py         # 6 Spark SQL queries + Excel export + S3 upload
│   └── smtp.py             # Provider-agnostic email sender
├── docker-compose.yml      # Spark cluster (1 master + 4 workers + Jupyter)
├── Dockerfile.jupyter      # JupyterLab image based on Spark 4.2.0
├── env.example             # Environment variables template
├── import_dataset.txt      # Kaggle download snippet
└── LICENSE                 # MIT
```

## Dependencies

- `pyspark` 4.2.0
- `boto3`
- `python-dotenv`
- `pandas`
- `openpyxl`

## Output

The pipeline generates an Excel file (`rba_analysis_YYYY-MM-DD.xlsx`) with one sheet per analysis query, uploaded to `s3://sparkanalysisauth/output/` and sent as an email attachment.

## License

MIT - see [LICENSE](LICENSE)
