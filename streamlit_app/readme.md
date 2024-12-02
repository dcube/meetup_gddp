# ⚡TPCH-SF100 Data App Benchmark⚡

## 🌟 Overview
This benchmark aims to measure the performance of loading data and executing the 22 TPC-H queries using two different data formats: native Snowflake and Iceberg. The benchmark evaluates the performance of these formats under different workloads, both in parallel and sequential execution.

## 📋 Data Model
The data model for this benchmark is based on the TPC-H standard schema. The schema includes multiple tables with relationships designed to simulate a real-world business environment. Below is a visual representation of the data model:

- **Customer**: Information about customers.
- **Orders**: Details of customer orders.
- **Lineitem**: Line items for each order.
- **Part**: Information about parts.
- **Supplier**: Details about suppliers.
- **Partsupp**: Information about parts supplied by suppliers.
- **Nation**: Information about nations.
- **Region**: Details about regions.

<img src="data:assets/tpch-100_data_model.png charset=utf-8;base64" alt="TPCH Data Model" width="600"/>

## 🏃Benchmark Details

### Data Loading
- **Data Scale**: TPCH-SF100
- **Data Formats**: Native Snowflake, Iceberg
- **Loading Process**: Data is loaded into Snowflake using both formats to prepare for query execution.

### Query Execution
- **Queries**: 22 TPC-H standard queries
- **Execution Modes**:
  - **Parallel Execution**: All queries are run simultaneously to measure the system's ability to handle concurrent workloads.
  - **Sequential Execution**: Queries are run one after another to measure the performance in a single-threaded environment.

## 🎯 Objectives
- **Performance Measurement**: Compare the execution times of the 22 TPC-H queries between the native Snowflake format and the Iceberg format.
- **Workload Analysis**: Assess how each format performs under parallel and sequential query execution.

## 📈 Results
The results of this benchmark will provide insights into:
- The efficiency of data loading for each format.
- The query performance differences between native Snowflake and Iceberg formats.
- The impact of parallel versus sequential execution on query performance.
