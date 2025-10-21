# E-Store Marketing Analytics 

[![CI](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml)
[![CD](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml)

**Status:** Work in progress

## Overview

Marketing analytics for a large [eCommerce events dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) from [REES46 Marketing Platform](https://rees46.com/). 

<img width="700" height="352" alt="Dagster dbt Asset Lineage Graph" src="https://github.com/user-attachments/assets/f2064b9e-6b12-4bf5-952e-0dae08c3c2ab" />

**Goal:** Produce actionable insights and metrics including customer LTV, churn rate, and purchase behavior.

## Pipeline Architecture

1. **Raw Data** → CSV files manually placed in Google Cloud Storage
2. **Data Warehouse** → BigQuery
3. **Transformation** → dbt Core
4. **Orchestration** → Dagster

## Roadmap
- [X] Automate GCS → BigQuery loading
- [ ] Complete dbt models for key metrics
- [ ] Build analytics dashboards
