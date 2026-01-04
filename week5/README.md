# Week 5: Cloud-Native ETL Pipeline on GCP

## 1. Introduction

### From Local Spark Processing to Cloud-Native Pipelines

In Week 4, we built a Spark-based mini ETL project to process place reviews, focusing on:

- Structured transformations
- Lazy execution
- Aggregation and partitioned outputs

In Week 5, we extend this logic into a **cloud-native data engineering pipeline on Google Cloud Platform (GCP)**.
Instead of batch processing with Spark, we move toward **event-driven ingestion, serverless compute, and cloud data warehousing**.

This reflects a realistic progression in data engineering:

```
Local / Spark-based ETL
        ↓
Event-driven Cloud ETL
        ↓
Cloud Storage + Serverless + Data Warehouse
```

---

## 2. Technologies Used

| Component | Service |
|-----------|---------|
| Object Storage | Google Cloud Storage (GCS) |
| Serverless Compute | Cloud Functions (Gen 2) |
| Event Trigger | Eventarc (GCS Object Finalize) |
| Messaging (internal) | Pub/Sub (managed by Eventarc) |
| Data Warehouse | BigQuery |
| Language | Python 3.11 |
| Format | JSON (input), CSV (staging) |

---

## 3. Project Overview: Reviews Ingestion Pipeline

### Project Goal

Build an **event-driven ETL pipeline** that:

- Automatically processes raw review data uploaded to cloud storage
- Transforms and stages clean data
- Loads structured data into a data warehouse for analytics

This project focuses on cloud ingestion patterns, not heavy computation (which was covered in Week 4).

---

## 4. Business Logic

### Input Data

**Raw Reviews (JSON)**

Uploaded to: `gs://<bucket-name>/raw/`

Example record:

```json
{
  "author_name": "An",
  "rating": 5,
  "review_text": "Very good, will return again..."
}
```

### Transformation Rules

| Rule | Description |
|------|-------------|
| Field selection | Keep only `author_name` and `rating` |
| Field removal | Drop `review_text` |
| Validation | Skip records missing required fields |
| Output format | CSV |
| Output location | `staging/` |
| File naming | Timestamp-based |

### Processing Flow

```
Raw JSON uploaded to GCS (raw/)
        ↓ (Eventarc trigger)
Cloud Function (Gen 2)
        ↓
[1] Parse JSON
[2] Extract author_name, rating
[3] Write CSV to staging/
[4] Move original file to processed/
        ↓
BigQuery Load
        ↓
Staging_Reviews table
```

---

## 5. Project Structure

```
week5/
├── data/
│   └── reviews_raw.json          # Sample input data
├── function/
│   ├── main.py                   # Cloud Function source code
│   └── requirements.txt          # Python dependencies
├── local_test/
│   ├── transform_local.py        # Local transform prototype
│   └── out.csv                   # Local test output
└── README.md
```

---

## 6. Code Components

### 6.1 Local Prototype (`local_test/transform_local.py`)

Before deploying to the cloud, transformation logic is validated locally.

**Responsibilities:**

- Read JSON file
- Extract required fields
- Output CSV

This mirrors the **"test locally first"** best practice in real data engineering workflows.

### 6.2 Cloud Function (`function/main.py`)

#### Entry Point

```python
def gcs_trigger(event, context):
```

Triggered when a new object is finalized in the GCS bucket.

**Responsibilities**

| Task | Description |
|------|-------------|
| Event Validation | Process only objects under `raw/` |
| Data Parsing | Read JSON content, support single object or list |
| Transformation | Extract `author_name`, `rating`, skip invalid records |
| Staging Output | Write CSV to `staging/` |
| Lifecycle Management | Move original file to `processed/` |

### 6.3 Dependencies (`requirements.txt`)

```
google-cloud-storage==2.19.0
```

---

## 7. Cloud Resources Setup

### Google Cloud Storage

Bucket structure:

```
gs://<bucket-name>/
├── raw/        # Incoming data
├── staging/    # Transformed output
└── processed/  # Archived raw files
```

### Cloud Function (Gen 2)

| Setting | Value |
|---------|-------|
| Runtime | Python 3.11 |
| Trigger | GCS Object Finalized |
| Region | asia-southeast1 |
| Entry Point | `gcs_trigger` |

Event handling is managed by **Eventarc**, which internally uses **Pub/Sub**.

### IAM (Key Learning Point)

To enable GCS → Eventarc → Cloud Function, the Cloud Storage service account requires:

- `roles/pubsub.publisher`
- `roles/pubsub.subscriber`
- `roles/eventarc.eventReceiver`

This highlights the importance of **service agents and IAM** in cloud-native pipelines.

---

## 8. Data Warehouse: BigQuery

### Dataset

`week5`

### Table

`Staging_Reviews`

### Schema

| Column | Type |
|--------|------|
| `author_name` | STRING |
| `rating` | INTEGER |

### Loading Data

Data is loaded from: `gs://<bucket-name>/staging/*.csv`

**Load operation:**

- CSV format
- Skip header row
- Append or truncate mode

---

## 9. Verification & Validation

### Check Function Execution

Cloud Functions → Logs

**Confirm:**

- File written to `staging/`
- Raw file moved to `processed/`

### Check BigQuery Data

```sql
SELECT * 
FROM `project_id.week5.Staging_Reviews`
LIMIT 10;
```

**Schema verification:**

```sql
SELECT column_name, data_type
FROM `project_id.week5.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'Staging_Reviews';
```

**Expected result:**

- Only `author_name`, `rating` columns present

---

## 10. Cost Considerations

This project stays well within **GCP Free Tier:**

- ✅ Cloud Storage: negligible usage
- ✅ Cloud Functions: low invocation count
- ✅ Eventarc & Pub/Sub: internal, minimal volume
- ✅ BigQuery: free load + small queries

**Designed for learning without cost risk.**

---

## Key Learnings

✨ **Event-driven architecture**: Serverless = pay per invocation
✨ **Cloud storage patterns**: raw → staging → processed lifecycle
✨ **IAM best practices**: Service accounts and role-based access
✨ **Eventarc integration**: GCS events → Cloud Functions
✨ **Data warehouse integration**: Staging patterns before analytics
✨ **Progression**: From batch (Spark) to streaming (Eventarc)

1. Introduction
From Local Spark Processing to Cloud-Native Pipelines

In Week 4, we built a Spark-based mini ETL project to process place reviews, focusing on:

Structured transformations

Lazy execution

Aggregation and partitioned outputs

In Week 5, we extend this logic into a cloud-native data engineering pipeline on Google Cloud Platform (GCP).
Instead of batch processing with Spark, we move toward event-driven ingestion, serverless compute, and cloud data warehousing.

This reflects a realistic progression in data engineering:

Local / Spark-based ETL
        ↓
Event-driven Cloud ETL
        ↓
Cloud Storage + Serverless + Data Warehouse

2. Technologies Used
Component	Service
Object Storage	Google Cloud Storage (GCS)
Serverless Compute	Cloud Functions (Gen 2)
Event Trigger	Eventarc (GCS Object Finalize)
Messaging (internal)	Pub/Sub (managed by Eventarc)
Data Warehouse	BigQuery
Language	Python 3.11
Format	JSON (input), CSV (staging)
3. Project Overview: Reviews Ingestion Pipeline
Project Goal

Build an event-driven ETL pipeline that:

Automatically processes raw review data uploaded to cloud storage

Transforms and stages clean data

Loads structured data into a data warehouse for analytics

This project focuses on cloud ingestion patterns, not heavy computation (which was covered in Week 4).

4. Business Logic
Input Data

Raw Reviews (JSON)
Uploaded to:

gs://<bucket-name>/raw/


Example record:

{
  "author_name": "An",
  "rating": 5,
  "review_text": "Very good, will return again..."
}

Transformation Rules
Rule	Description
Field selection	Keep only author_name and rating
Field removal	Drop review_text
Validation	Skip records missing required fields
Output format	CSV
Output location	staging/
File naming	Timestamp-based
Processing Flow
Raw JSON uploaded to GCS (raw/)
        ↓ (Eventarc trigger)
Cloud Function (Gen 2)
        ↓
[1] Parse JSON
[2] Extract author_name, rating
[3] Write CSV to staging/
[4] Move original file to processed/
        ↓
BigQuery Load
        ↓
Staging_Reviews table

5. Project Structure
week5/
├── data/
│   └── reviews_raw.json          # Sample input data
├── function/
│   ├── main.py                   # Cloud Function source code
│   └── requirements.txt          # Python dependencies
├── local_test/
│   ├── transform_local.py        # Local transform prototype
│   └── out.csv                   # Local test output
└── README.md

6. Code Components
6.1 Local Prototype (local_test/transform_local.py)

Before deploying to the cloud, transformation logic is validated locally.

Responsibilities:

Read JSON file

Extract required fields

Output CSV

This mirrors the “test locally first” best practice in real data engineering workflows.

6.2 Cloud Function (function/main.py)
Entry Point
def gcs_trigger(event, context):


Triggered when a new object is finalized in the GCS bucket.

Responsibilities

Event Validation

Process only objects under raw/

Data Parsing

Read JSON content

Support single object or list

Transformation

Extract author_name, rating

Skip invalid records

Staging Output

Write CSV to staging/

Lifecycle Management

Move original file to processed/

6.3 Dependencies (requirements.txt)
google-cloud-storage==2.19.0

7. Cloud Resources Setup
Google Cloud Storage

Bucket structure:

gs://<bucket-name>/
├── raw/        # Incoming data
├── staging/    # Transformed output
└── processed/  # Archived raw files

Cloud Function (Gen 2)
Setting	Value
Runtime	Python 3.11
Trigger	GCS Object Finalized
Region	asia-southeast1
Entry Point	gcs_trigger

Event handling is managed by Eventarc, which internally uses Pub/Sub.

IAM (Key Learning Point)

To enable GCS → Eventarc → Cloud Function, the Cloud Storage service account requires:

roles/pubsub.publisher

roles/pubsub.subscriber

roles/eventarc.eventReceiver

This highlights the importance of service agents and IAM in cloud-native pipelines.

8. Data Warehouse: BigQuery
Dataset
week5

Table
Staging_Reviews

Schema
Column	Type
author_name	STRING
rating	INTEGER
Loading Data

Data is loaded from:

gs://<bucket-name>/staging/*.csv


Load operation:

CSV format

Skip header row

Append or truncate mode

9. Verification & Validation
Check Function Execution

Cloud Functions → Logs

Confirm:

File written to staging/

Raw file moved to processed/

Check BigQuery Data
SELECT * 
FROM `project_id.week5.Staging_Reviews`
LIMIT 10;


Schema verification:

SELECT column_name, data_type
FROM `project_id.week5.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'Staging_Reviews';


Expected result:

Only author_name, rating

10. Cost Considerations

This project stays well within GCP Free Tier:

Cloud Storage: negligible usage

Cloud Functions: low invocation count

Eventarc & Pub/Sub: internal, minimal volume

BigQuery: free load + small queries

Designed for learning without cost risk.