# E-commerce Analytics Pipeline

[![Terraform](https://github.com/vbalalian/estore-analytics/actions/workflows/terraform.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/terraform.yml)
[![CI - dbt](https://github.com/vbalalian/estore-analytics/actions/workflows/ci-dbt.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/ci-dbt.yml)
[![CI - Dagster](https://github.com/vbalalian/estore-analytics/actions/workflows/ci-dagster.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/ci-dagster.yml)
[![CD](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml)

## Overview

Marketing analytics for a large [eCommerce events dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) from [REES46 Marketing Platform](https://rees46.com/).

![Raw Events Snippet](/analysis/images/screenshots/estore-raw-events-snippet.png)

A production-style data pipeline that processes 400M+ e-commerce events to generate customer analytics and business insights. Built using modern data engineering tools (dbt, Dagster, BigQuery) with an Omni Analytics semantic layer for interactive analysis.

## Contents
- [Pipeline Architecture](#pipeline-architecture)
- [Key Findings](#key-findings)
- [Tech Stack](#tech-stack)
- [Data Models](#data-models)
- [Semantic Layer](#semantic-layer)
- [Getting Started](#getting-started)
- [Documentation](#documentation)

## Pipeline Architecture

![Pipeline Architecture](/docs/diagrams/pipeline.svg)

## Key Findings

- **RFM Segmentation**: Champions (12% of customers) generate ~40% of revenue ($830M). 135K high-value "At Risk" customers identified for retention, with $330M in revenue at stake.
- **Conversion Funnel**: 5.92% overall conversion rate. 88% drop-off before cart, ~50% cart abandonment. Returning customers convert at 4x the rate of first-time buyers.
- **Churn**: At Risk segment shows 78% churn rate despite averaging 21.7 sessions per customer, indicating high prior engagement. Needs Attention customers have the highest AOV ($564.71), making them the most cost-effective retention target.
- **Category Performance**: Construction dominates revenue ($1.33B) and conversion (2.78%), but has the largest cart abandonment gap (7.05% add rate vs 2.78% purchase rate), representing significant recoverable revenue.
---
![Customer Segmentation Deep Dive](/analysis/images/omni/Customer_Segmentation_Deep_Dive.png)
*Customer Segmentation workbook in Omni Analytics. See [full analysis](analysis/README.md) for all three workbooks.*

## Tech Stack
- **Data Warehouse**: BigQuery, serverless, native partitioning/clustering
- **Transformation**: dbt Core, version-controlled SQL, built-in testing, lineage tracking
- **Orchestration**: Dagster, asset-based paradigm, first-class dbt integration
- **BI Layer**: Omni Analytics, semantic modeling with bi-directional dbt integration, custom measures/dimensions in YAML
- **Infrastructure**: Google Cloud Platform, Terraform for IaC
- **CI/CD**: GitHub Actions with slim CI (state-based dbt builds), automated rollback, Omni schema refresh on deploy

## Data Models

**Staging Layer**
- `stg_events` - Deduplicated, cleaned event data with multi-brand product filtering

**Dimension Tables**
- `dim_users` - User-level metrics (LTV, churn status, activity classification, purchase history)
- `dim_products` - Product attributes and category hierarchy
- `dim_user_rfm` - RFM scores and customer segments (9 segments)

**Fact Tables**
- `fct_events` - Event-level facts with purchase/cart/view flags (incremental, partitioned by day)
- `fct_sessions` - Session-level aggregations with 1-hour timeout splitting and conversion funnel stage

**Snapshots (SCD Type 2)**
- `snap_user_rfm` - Tracks changes to RFM segments over time
- `snap_user_status` - Tracks changes to user activity/churn status

## Semantic Layer

The Omni Analytics semantic layer ([`omni/`](/omni/BigQuery/)) extends the dbt models with:

- **3 topics**: Sessions, Customers, Events, each with pre-configured join paths
- **Custom dimensions**: `rfm_segment_inclusive` (COALESCE to include non-purchasers), `session_quality_tier`, `customer_lifecycle_stage`, `session_length_bucket`
- **Custom measures**: Conversion rates (view-to-cart, cart-to-purchase), churn rate, purchaser rate, revenue per session, avg LTV
- **Bi-directional dbt sync**: Descriptions and metadata flow between dbt YAML and Omni on schema refresh (triggered automatically in CD pipeline)

See [Analysis & Visualizations](analysis/README.md) for workbook screenshots and detailed findings.

## Lineage Graph

![Dagster Asset Lineage Graph](/docs/diagrams/lineage.svg)

## dbt Lineage (with BI layer exposures)

![dbt Dag](/docs/diagrams/dbt-dag.png)

## Getting Started

### Prerequisites

- **Python** 3.9 - 3.13
- **Terraform** >= 1.0
- **gcloud CLI** authenticated with your GCP project
- **GCP Project** with BigQuery and Cloud Storage APIs enabled
- **Service Account** with roles: BigQuery Admin, Storage Admin

### Infrastructure Setup

Provision the required GCP resources using Terraform:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your GCP project ID, bucket name, and Slack token
terraform init
terraform apply
```

This provisions a GCP VM with Dagster running as systemd services. Access the Dagster UI via SSH tunnel:

```bash
gcloud compute ssh terraform-instance --zone=us-central1-a -- -L 3000:localhost:3000
# Then open http://localhost:3000 in your browser
```

See [terraform/README.md](terraform/README.md) for detailed setup instructions.

### dbt Configuration

Configure dbt to connect to your BigQuery instance:

```bash
cd dbt-project
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your GCP project ID and service account key path
```

### Running the Pipeline

1. Upload raw data CSV files to the GCS bucket
2. Dagster sensors automatically detect new files and trigger data loads
3. Transformations run automatically after successful loads

## Documentation

- [Analysis & Visualizations](analysis/README.md) - Omni workbook screenshots and detailed insights
- [Orchestration](docs/orchestration.md) - Dagster sensors, jobs, and scheduling patterns
- **dbt Docs** - Run `cd dbt-project && dbt docs generate && dbt docs serve` to view model documentation and lineage locally
