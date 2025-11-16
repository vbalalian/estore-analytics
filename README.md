# E-Store Marketing Analytics 

[![CI](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/ci.yml)
[![CD](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml/badge.svg)](https://github.com/vbalalian/estore-analytics/actions/workflows/cd.yml)

## Overview

Marketing analytics for a large [eCommerce events dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) from [REES46 Marketing Platform](https://rees46.com/). 

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
            E[Dagster<br/>Event-Driven Loading & Transformation]
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

## Roadmap
- [X] Automate GCS → BigQuery loading
- [X] Complete dbt models for key metrics
- [ ] Build analytics dashboards
