# E-Store Marketing Analytics 

**Status:** Work in progress

## Overview

Marketing analytics for a large [eCommerce events dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) from [REES46 Marketing Platform](https://rees46.com/). 

**Goal:** Produce actionable insights and metrics including customer LTV, churn rate, and purchase behavior.

## Pipeline Architecture

1. **Raw Data** → CSV files in Google Cloud Storage *(manual, will be automated)*
2. **Data Warehouse** → BigQuery *(manual load, will be automated)*
3. **Transformation** → dbt Core for staging and modeling
4. **Orchestration** → Dagster for scheduling dbt jobs

## Tech Stack
- Google Cloud Platform (Cloud Storage, BigQuery)
- dbt Core
- Dagster

## Roadmap
- [ ] Automate CSV ingestion to GCS
- [ ] Automate GCS → BigQuery loading
- [ ] Complete dbt models for key metrics
- [ ] Build analytics dashboards
