# Kisan Call Centre (KCC) Farmer Queries Processing Pipeline

A batch-processing data system designed to handle farmer query data from the Kisan Call Centre (KCC) made available by the Open Data Portal from the Indian Government. The system ingests, processes, and analyzes approximately 9 million records of farmer queries to provide valuable insights for agricultural planning and support.

## Architecture Diagram
![Architecture Diagram](docs/KCC%20Pipeline_v3.png)

## 🏗️ System Architecture

This pipeline follows a **microservices architecture** with three main components:

- **🔄 Ingestion Service**: Automated data extraction from KCC API with support for backfill and incremental loads
- **⚙️ Processing Service**: ETL operations transforming raw data into a star schema for analytics
- **📊 Visualization Service**: Interactive dashboards and analytics using Jupyter Lab

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- KCC API Key (optional for testing with sample data)

### Run the Complete Pipeline
```bash
# Clone the repository
git clone <repository-url>
cd kcc-farmer-queries-pipeline

# Start all services
docker-compose up
```

This will:
1. **Ingest** sample data (100 rows per month)
2. **Process** data into star schema format
3. **Launch** Jupyter Lab at http://localhost:8888 for visualization

### Individual Service Usage

#### 🔄 Data Ingestion
```bash
# Build ingestion service
docker build -f docker/ingestion.Dockerfile -t kcc-ingestion .

# Run backfill (limited sample)
docker run --rm -v $(pwd)/storage:/app/storage kcc-ingestion --backfill --max-offset 200

# Run for last month only
docker run --rm -v $(pwd)/storage:/app/storage kcc-ingestion
```

#### ⚙️ Data Processing
```bash
# Build processing service
docker build -f docker/processing.Dockerfile -t kcc-processing .

# Run initial load (backfill processing)
docker run --rm -v $(pwd)/storage:/app/storage kcc-processing python main.py --job initial

# Run incremental processing
docker run --rm -v $(pwd)/storage:/app/storage kcc-processing python main.py --job incremental
```

#### 📊 Visualization
```bash
# Build visualization service
docker build -f docker/visualize.Dockerfile -t kcc-visualize .

# Launch Jupyter Lab
docker run --rm -p 8888:8888 -v $(pwd)/storage:/app/storage -v $(pwd)/visualization/notebooks:/app/notebooks kcc-visualize
```

## 📁 Project Structure

```
kcc-farmer-queries-pipeline/
├── docker/                     # Container definitions
├── ingestion/                  # Data ingestion microservice
├── processing/                 # ETL and data transformation
├── visualization/              # Analytics dashboards
├── storage/                    # Data storage (raw + processed)
├── docs/                       # Architecture diagrams
└── docker-compose.yml          # Orchestration configuration
```

## 🗄️ Data Model

The system implements a **star schema** for efficient analytics:

- **Fact Table**: `fct_queries` (9M+ farmer queries)
- **Dimensions**: `dim_demography`, `dim_category`, `dim_sector`
- **Storage**: DuckDB for high-performance analytics queries

## 🔧 Key Features

- **🔄 Dual Processing Modes**: Backfill historical data or process incrementally
- **🛡️ Data Quality**: PII masking, null handling, and data validation
- **📈 Performance Optimized**: Parquet format, row limits, and efficient querying
- **🐳 Containerized**: Complete Docker-based deployment
- **📊 Rich Analytics**: Geographic, temporal, and categorical insights

## 🎯 Use Cases

- **Agricultural Policy Planning**: State and district-level farmer needs analysis
- **Crop Support Programs**: Identify common issues by crop type and region
- **Resource Allocation**: Understand query patterns by sector and category
- **NLP Applications**: Clean, structured text data for machine learning models

## 📊 Sample Analytics

The visualization service provides insights on:
- Geographic distribution of farmer queries
- Seasonal patterns in agricultural concerns
- Category and sector breakdown analysis
- State vs. query type heatmaps

## 🛠️ Technical Stack

- **Data Processing**: Python, PySpark, DuckDB
- **Storage**: Parquet files, DuckDB database
- **Containerization**: Docker, Docker Compose
- **Visualization**: Jupyter Lab, Plotly, Matplotlib
- **Data Integration**: DLT (Data Load Tool)

## 📖 Documentation

- [Implementation Report](docs/implementation-report.md) - Detailed technical documentation
- [Architecture Diagrams](docs/) - Visual system overview
- [Data Model](docs/kcc_data_model.png) - Database schema reference

## 🚨 Important Notes

- Default ingestion limit: 50,000 rows per month (configurable)
- Sample mode processes 100 rows per month for quick testing
- Requires API key for full data access (contact Indian Government Open Data Portal). [Link to generate API Key](https://www.data.gov.in/resource/kisan-call-centre-kcc-transcripts-farmers-queries-answers)
- Processing ~9M records requires significant compute resources

