# E-Store Marketing Analytics 

[![CI](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml)
[![CD](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml)

## Overview

Marketing analytics for a large [eCommerce events dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) from [REES46 Marketing Platform](https://rees46.com/). 

<img width="1146" height="939" alt="Dagster Asset Lineage Graph" src="https://github.com/user-attachments/assets/9e27e52f-b896-4604-9813-371272553766" />

## Pipeline Architecture

1. **Raw Data** → CSV files manually placed in Google Cloud Storage
2. **Data Warehouse** → BigQuery
3. **Transformation** → dbt Core
4. **Orchestration** → Dagster

## Roadmap
- [X] Automate GCS → BigQuery loading
- [X] Complete dbt models for key metrics
- [ ] Build analytics dashboards
