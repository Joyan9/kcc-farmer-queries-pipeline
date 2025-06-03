# Kisan Call Centre (KCC) Farmer Queries Processing Pipeline

A batch-processing data system designed to handle farmer query data from the Kisan Call Centre (KCC) made available by the Open Data Portal from the Indian Government. The system ingests, processes, and analyzes approximately 9 million records of farmer queries to provide valuable insights for agricultural planning and support.

## Architecture Diagram

![Architecture Diagram](docs/architecture/KCC%20Pipeline_v3.png)

## 🏗️ System Architecture

This pipeline follows a **microservices architecture** with four main components:

* **🔄 Ingestion Service**: dlt-based data extraction from KCC API with support for backfill and incremental loads
* **⚙️ Processing Service**: Processing the raw data and transforming it into a star schema for analytics
* **📊 Visualization Service**: Interactive dashboards and analytics using Jupyter Lab
* **🧪 Testing Service**: Test suite validating ingestion, processing logic, configurations, and end-to-end pipeline behavior

## 🚀 Quick Start

### Prerequisites

* Docker and Docker Compose
* KCC API Key ➡️ [Link to generate API Key](https://www.data.gov.in/resource/kisan-call-centre-kcc-transcripts-farmers-queries-answers)

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/Joyan9/kcc-farmer-queries-pipeline.git
   cd kcc-farmer-queries-pipeline
   ```

2. **Set up your API key**
   
   Create a `.env` file in the project root directory:
   ```bash
   # .env file example
   KCC_API_KEY=1234566
   # no need of quotes, keep the name of the env var same
   ```

3. **Start all services**
   ```bash
   docker-compose up
   ```

### Run the Complete Pipeline

Once you've completed the setup steps above, the pipeline will start automatically when you run `docker-compose up`.

This will:

1. **Ingest** sample data (100 rows per month)
2. **Process** data into star schema format
3. **Run** the test suite after processing completes
4. **Launch** Jupyter Lab at [http://localhost:8888](http://localhost:8888) for visualization

### Individual Service Usage

#### 🧪 Testing

```bash
# Build testing service
docker build -f docker/testing.dockerfile -t kcc-testing .

# Run tests (unit + integration)
docker run --rm -v $(pwd)/storage:/app/storage kcc-testing
```

Includes:

* Unit tests for ingestion helpers and argument parsing
* Spark-based cleaning and dimension logic tests
* Integration tests for pipeline configuration and DuckDB output

## 📁 Project Structure

```
kcc-farmer-queries-pipeline/
├── docker/                     # Container definitions
├── ingestion/                  # Data ingestion microservice
├── processing/                 # ETL and data transformation
├── visualization/              # Analytics dashboards
├── tests/                      # Unit and integration test suite
├── storage/                    # Data storage (raw + processed)
├── docs/                       # Architecture diagrams
└── docker-compose.yml          # Orchestration configuration
```

Here’s the corrected and polished version of the **📖 Documentation** section with consistent formatting, a placeholder for the finalization report, and the test coverage details clearly separated:

---

## 📖 Documentation

* [Conception Report](docs/Bhathena-Joyan_9213297_Data%20Engineering_P1_S.pdf) – Documentation for the conception phase of the project
* [Implementation Report](docs/Bhathena-Joyan_9213297_Data%20Engineering_P2_S.pdf) – Detailed technical documentation for the implementation phase
* *Finalization Report* – *(To be added)*
* [Architecture Diagrams](docs/architecture/) – Visual system overview
* [Data Model](docs/kcc_data_model.png) – Database schema reference

**Test Coverage:**
The testing service covers core components such as:

* Ingestion logic
* Data cleaning routines
* Dimension table generation
* Configuration file validation
* End-to-end pipeline validation with DuckDB

## 🚨 Important Notes

- Default ingestion limit: 50,000 rows per month (configurable)
- Sample mode processes 100 rows per month for quick testing
- Requires API key for full data access. [Link to generate API Key](https://www.data.gov.in/resource/kisan-call-centre-kcc-transcripts-farmers-queries-answers) 
- Processing ~9M records requires significant compute resources
