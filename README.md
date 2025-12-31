# 📈 Stock Market Big Data Analysis with Apache Spark

A Big Data pipeline built with **Scala** and **Apache Spark** to analyze high-frequency financial data (Parquet), perform clustering, and visualize market regimes.

## 📋 Project Overview

The goal is to analyze stock market order book data to extract insights such as liquidity, volatility, and bid-ask spread. The project includes a full ETL pipeline, advanced window functions, K-Means clustering, and a real-time dashboard.

### 🏗️ Architecture

```mermaid
graph TD
    A[Parquet Data] --> B[Spark Ingestion (Scala)]
    B --> C[Cleaning & Feature Engineering]
    C --> D[Window Functions (Temporal Analysis)]
    D --> E[MLlib K-Means Clustering]
    E --> F[Text Reports (output/)]
    F --> G[Streamlit Dashboard (Python)]
```

### 📂 Project Structure

- **`src/main/scala/`**: Scala Spark Code
  - `StockPhase1.scala`: Ingestion & Initial Exploration.
  - `StockPhase2.scala`: Cleaning & Descriptive Statistics.
  - `StockPhase3.scala`: Temporal Analysis (Window Functions).
  - `StockPhase4.scala`: Machine Learning (K-Means Clustering).
- **`app.py`**: Streamlit Dashboard for data visualization.
- **`data/`**: dataset input (`stock_id.parquet`).
- **`output/`**: generated text reports.

## 🛠️ Usage

### 1. Run Spark Phases (Scala)

Use `sbt` to run specific phases of the pipeline:

```bash
# Phase 1: Ingestion
sbt "runMain StockPhase1"

# Phase 2: Descriptive Analysis
sbt "runMain StockPhase2"

# Phase 3: Temporal Analysis (Window Functions)
sbt "runMain StockPhase3"

# Phase 4: Machine Learning (Clustering)
sbt "runMain StockPhase4"
```

Results are generated in the `output/` folder (e.g., `stock_phase4_results.txt`).

### 2. Run Dashboard (Python)

Visualize the results and explore the data interactively:

```bash
streamlit run app.py
```

## 📊 Features

- **Bid-Ask Spread Analysis**: Calculation and visualization of spreads over time.
- **Liquidity Metrics**: Analysis of bid/ask sizes.
- **Market Regimes**: Identification of market states (Calm, Active, Stress) using Unsupervised Learning (K-Means).
- **Interactive Visualization**: Dynamic charts using Plotly and Streamlit.

## 🛡️ Technologies

- **Apache Spark 3.5.0** (Scala)
- **SBT** (Build Tool)
- **Python 3.x**
- **Streamlit & Plotly**
- **Parquet** (Data Format)
