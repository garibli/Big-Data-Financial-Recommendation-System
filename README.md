# 📊 Financial Asset Ranking System (Apache Spark)

This project is a **Big Data analytics pipeline built with Java and Apache Spark** that processes large-scale financial market data to identify and rank profitable investment opportunities.

The system performs distributed computation over millions of stock price records and outputs the **Top 5 assets** based on recent performance and risk constraints.

---

## 🚀 Overview

The application analyzes historical stock prices and asset metadata to compute key financial indicators:

* **Return on Investment (ROI)** over the last 5 days
* **Volatility** over a 1-year window (~251 trading days)

It then filters and ranks assets to provide a final investment recommendation.

---

## ⚙️ Processing Pipeline

The system is implemented as a **Spark transformation pipeline**:

1. **Load Data**

   * Stock price data (CSV)
   * Asset metadata (JSON)

2. **Preprocessing**

   * Convert raw rows into domain objects (`StockPrice`)
   * Remove invalid/missing price entries

3. **Grouping**

   * Group stock prices by ticker symbol

4. **Feature Engineering**

   * Sort prices by time
   * Compute:

     * ROI (5-day return)
     * Volatility (1-year window)

5. **Filtering**

   * Remove high-risk assets (Volatility ≥ 4)
   * Remove overvalued assets (P/E ≥ 25)

6. **Join with Metadata**

   * Attach asset details (sector, industry, P/E ratio)

7. **Ranking**

   * Sort by return (descending)
   * Select **Top 5 assets**

---

## 🧠 Key Implementation Details

* Uses **Apache Spark (RDD + Dataset APIs)**
* Custom Spark transformations:

  * Map, Filter, Pair, Reduce operations
* Efficient **time-series processing**

  * Sorting by timestamp
  * Sliding windows for financial indicators
* Modular object-oriented design:

  * `Asset`, `AssetFeatures`, `StockPrice`, `AssetMetadata`
* Utility classes:

  * `MathUtils` (statistical operations)
  * `TimeUtil` (date parsing and handling)

---

## 🏗️ Project Structure

```
src/
 └── bigdata/
     ├── app/                  # Main Spark pipeline
     ├── objects/              # Domain models
     ├── technicalindicators/  # Returns & Volatility
     ├── transformations/
     │   ├── aggregation/
     │   ├── comparators/
     │   ├── filters/
     │   ├── maps/
     │   ├── pairing/
     │   └── reducers/
     └── util/                 # Math & time utilities
```

---

## 🧪 Technologies

* Java (JDK 21)
* Apache Spark
* Maven
* Distributed Computing
* Big Data Processing

---

## 📈 Dataset

* Historical stock prices (~24M records, ~2.4GB)
* Asset metadata (JSON)

⚠️ Dataset is **not included** in this repository due to size limitations.

---

---

## 🎯 Learning Outcomes

This project demonstrates:

* Designing scalable big data pipelines
* Working with distributed systems (Apache Spark)
* Processing financial time-series data
* Writing clean, modular Java code

---

## 📌 Notes

* The project is designed for batch processing using Apache Spark
* Large dataset is excluded from the repository
* Hadoop local binaries (winutils) are not required for deployment

---

## 👨‍💻 Author

**Fuad Garibli**
MSc Data Science – University of Glasgow

---
