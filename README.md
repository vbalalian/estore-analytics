# E-commerce Analytics Pipeline 

[![CI](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml)
[![CD](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml)

## Overview

Marketing analytics for a large [eCommerce events dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) from [REES46 Marketing Platform](https://rees46.com/).

![Raw Events Snippet](/images/screenshots/estore-raw-events-snippet.png)

* A production-style data pipeline that processes 400M+ e-commerce events to generate customer analytics and business insights.
* Built using modern data engineering tools (dbt, Dagster, BigQuery) to demonstrate scalable analytics infrastructure and best practices.
* The pipeline automates data ingestion, transformation, and metric calculation for customer segmentation (RFM analysis), conversion funnel tracking, churn identification, and other KPIs.

## Key Findings

**Customer Segmentation (RFM Analysis)**
- Champions segment (12% of customers) generates average revenue of $3,333, over 3x higher than typical customers
- Identified 135K "At Risk" customers with high historical value ($2,452 avg) who haven't purchased recently
- Lost customers (15%) represent only $110 average revenue (minimal recovery value)
- **Business Impact**: RFM segmentation enables targeted retention campaigns for high-value customers, potentially recovering significant revenue from the "At Risk" segment.

**Churn Analysis (Cohort-Based)**
- **88% of early customers did not make a repeat purchase within 90 days** - indicating significant retention challenges
- October 2019 cohort showed 82% churn rate; November cohort 94%
- Analysis based on customers with sufficient time in dataset to exhibit repeat purchase behavior
- **Business Impact**: High one-time buyer rate suggests critical need for post-purchase engagement, loyalty programs, and retention campaigns

## Pipeline Architecture

```mermaid
graph TB
    subgraph "Data Source"
        A[Kaggle Dataset<br/>100M+ Events]
    end
    
    subgraph "Google Cloud Platform"
        B[Cloud Storage<br/>CSV Files]
        C[BigQuery<br/>Data Warehouse]
        
        subgraph "Transformation Layer"
            D[dbt Core<br/>Staging & Marts]
        end
        
        subgraph "Orchestration"
            E[Dagster<br/>Loading & Transformation]
        end
        
        F[Compute Engine<br/>Virtual Machine]
    end
    
    subgraph "Analytics Output"
        G[Customer LTV]
        H[Churn Metrics]
        I[RFM Analysis]
    end
    
    A -->|Manual Load| B
    B -->|Load| C
    C -->|Raw Data| D
    D -->|Transformed Models| G
    D -->|Transformed Models| H
    D -->|Transformed Models| I
    E -.->|Orchestrates| D
    F -.->|Hosts| E
    F -.->|Runs| D
    
    style A fill:#e1f5ff
    style B fill:#fff4e6
    style C fill:#fff4e6
    style D fill:#e8f5e9
    style E fill:#f3e5f5
    style G fill:#fce4ec
    style H fill:#fce4ec
    style I fill:#fce4ec
```

## Lineage Graph

![Dagster Asset Lineage Graph](/images/screenshots/Global_Asset_Lineage.svg)

## Data Visualizations

### Dashboard - Sales & AOV
![Dashboard - Sales & AOV](/images/data_viz/dashboard_sales_aov.png)

### Dashboard - Churn & LTV
![Dashboard - Churn & LTV](/images/data_viz/dashboard_churn_and_ltv.png)

### Total Revenue by RFM Segment
![Total Revenue by Segment](/images/data_viz/Total_Revenue_by_Segment.png)

### Average Revenue & Population by RFM Segment
![Average Revenue & Population by RFM Segment - 1](/images/data_viz/RFM_Segment_Avg_Revenue_&_Population.png)

![Average Revenue & Population by RFM Segment - 2](/images/data_viz/RFM_Segment_Revenue_&_Population_bars.png)
