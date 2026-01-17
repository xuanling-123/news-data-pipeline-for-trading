# news-data-pipeline-for-trading

## The Value of Unstructured News Data
In algorithmic trading, unstructured news data provides a crucial edge that structured market data alone cannot deliver. News articles contain forward-looking information about corporate events, economic shifts, and market sentiment that precede price movements. This pipeline transforms unstructured text into actionable trading signals through sentiment analysis and entity extraction, enabling systematic strategies to incorporate the same news-driven insights that fundamental analysts use manually. The ability to process thousands of news sources in real-time creates opportunities for event-driven trading, risk management, and alpha generation.

## Project Overview
This project implements a comprehensive news data pipeline for trading analysis using Databricks. The pipeline extracts news articles from multiple sources, transforms the data for analysis, and aggregates insights to support trading decisions.

## Architecture
The pipeline follows a modular ETL (Extract, Transform, Load) architecture with four main components:

```
news-data-for-trading/
├── 1-setup/
│   └── 1-setup.py              # Environment setup and configuration
├── 2-extractor/
│   └── news_extractor.py       # News data extraction from sources
├── 3-transformer/
│   └── news_transformer.py     # Data transformation and cleaning
└── 4-Aggregation/
    └── news_aggregator.py      # Data aggregation and analysis
```

## Components
1. Setup (1-setup/1-setup.py)
  - Initializes the Databricks environment
  - Configures necessary libraries and dependencies
  - Sets up database connections and storage locations
2. Extractor (2-extractor/news_extractor.py)
  - Extracts news articles from configured sources
  - Handles API authentication and rate limiting
  - Stores raw news data in structured format
3. Transformer (3-transformer/news_transformer.py)
  - Cleans and normalizes extracted news data
  - Performs text processing and sentiment analysis
  - Structures data for downstream analysis
4. Aggregator (4-Aggregation/news_aggregator.py)
  - Aggregates transformed data by various dimensions
  - Generates trading signals and insights
  - Produces summary statistics and reports

## Analytics Dashboard

The pipeline generates comprehensive analytics visualizations including:

<img width="1583" height="736" alt="image" src="https://github.com/user-attachments/assets/e2cb2d37-f6cc-4819-b89e-1dd35da12483" />

### Key Metrics Displayed:
- **Total Articles Today**: Real-time article volume tracking (2.25K articles)
- **Average Sentiment**: Market sentiment scoring across all sources (0.08)
- **High Impact Alerts**: Automated detection of market-moving news events
- **Sentiment Over Time**: Trend analysis for Bitcoin, Gold, and Silver
- **Commodity Analysis**: Article distribution and sentiment by commodity
- **Recent Headlines**: Live feed of analyzed articles with sentiment classification

> **⚠️ Note on Data Availability**  
> This project currently uses the free tier of NewsAPI, which limits API requests per day

## Features
- Multi-source Integration: Extract news from various financial news APIs
- Real-time Processing: Process news data as it becomes available
- Sentiment Analysis: Analyze news sentiment for trading signals
- Scalable Architecture: Built on Databricks for handling large volumes
- Modular Design: Easy to extend with additional sources or transformations

## Use Cases
- Trading Signal Generation: Identify news-driven trading opportunities
- Market Sentiment Analysis: Track overall market sentiment trends
- Risk Management: Monitor news for potential market-moving events
- Research & Backtesting: Historical news data for strategy development

