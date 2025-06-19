# SQL Server Data Access Performance Benchmark Results

## Test Environment

**System:** Ubuntu 20.04, Python 3.11.13, 12 CPU cores, 31.1GB RAM  
**Database:** SQL Server (local connection)  
**Data Types:** BIGINT, DATETIME2(0), DECIMAL(9,2)  
**Table Size:** 401,983,740 total records  
**Test Date:** 2025-06-19

## Performance Summary

### Performance by Dataset Size

| Method | 100K rows | 1M rows | 10M rows | Performance Ranking |
|--------|-----------|---------|----------|-------------------|
| **Polars Native** | 0.08s | 0.23s | 2.31s | 🥇 **Winner** |
| **ConnectorX → Polars Direct** | 0.09s | 0.88s | 8.73s | 🥈 2nd |
| **ConnectorX → Arrow → Polars** | 0.09s | 0.90s | 8.74s | 🥉 3rd |
| **ConnectorX → Pandas** | 0.09s | 0.91s | 9.22s | 4th |
| **pyodbc → Pandas** | 0.22s | 2.20s | 21.86s | 5th |
| **SQLAlchemy → Pandas** | 0.33s | 3.24s | 33.03s | 6th |

### Memory Usage by Dataset Size

| Method | 100K rows | 1M rows | 10M rows |
|--------|-----------|---------|----------|
| **Polars Native** | 178 MB | 254 MB | 937 MB |
| **ConnectorX → Polars Direct** | 183 MB | 283 MB | 1,065 MB |
| **ConnectorX → Arrow → Polars** | 183 MB | 274 MB | 1,056 MB |
| **ConnectorX → Pandas** | 157 MB | 210 MB | 515 MB |
| **pyodbc → Pandas** | 191 MB | 323 MB | 1,313 MB |
| **SQLAlchemy → Pandas** | 196 MB | 354 MB | 1,338 MB |

## Key Performance Insights

### Small Datasets (100K rows)
- **All modern methods perform similarly** (~0.08-0.09s)
- **Traditional methods 2-4x slower** but still very fast
- **Memory usage relatively consistent** across methods

### Medium Datasets (1M rows)
- **Polars Native dominates** at 0.23s (3.8x faster than nearest competitor)
- **ConnectorX methods cluster** around 0.88-0.91s
- **Traditional methods 2.4-3.6x slower** than ConnectorX
- **Clear performance separation** emerges between method categories

### Large Datasets (10M rows)
- **Polars Native maintains commanding lead** at 2.31s
- **ConnectorX methods scale** to 8.7-9.2s (3.8-4x slower than Polars)
- **Traditional methods become significantly slower** (21-33s, 9-14x slower than Polars)
- **Memory efficiency varies significantly** between methods

## Performance Scaling Analysis

### Scaling Efficiency (How performance changes with 10x more data)

| Method | 100K→1M (10x data) | 1M→10M (10x data) | Overall Scaling |
|--------|-------------------|-------------------|-----------------|
| **Polars Native** | 2.9x slower | 10.0x slower | ⭐ Excellent |
| **ConnectorX → Polars Direct** | 9.8x slower | 9.9x slower | ⭐ Excellent |
| **ConnectorX → Arrow → Polars** | 9.9x slower | 9.7x slower | ⭐ Excellent |
| **ConnectorX → Pandas** | 9.7x slower | 10.1x slower | ⭐ Excellent |
| **pyodbc → Pandas** | 9.8x slower | 9.9x slower | ⭐ Excellent |
| **SQLAlchemy → Pandas** | 9.8x slower | 10.2x slower | ⭐ Excellent |

**Note:** All methods show excellent linear scaling characteristics, suggesting efficient memory management and processing algorithms.

## Method Comparison Analysis

### Polars Native
- **Fastest across all dataset sizes**
- **Best scaling characteristics** for small to medium datasets
- **Moderate memory usage**
- **Excellent for production workloads**

### ConnectorX Methods
- **Consistent performance** across different output formats
- **Good scaling characteristics**
- **Polars Direct slightly faster** than Arrow conversion
- **Reliable choice for large datasets**

### Traditional Methods (pyodbc/SQLAlchemy)
- **Significantly slower** for large datasets
- **Higher memory usage**
- **Still viable for small datasets**
- **Familiar APIs for existing codebases**

## Recommendations

### For Small Datasets (<100K rows)
- **Any method works well** - choose based on ecosystem compatibility
- **Performance differences minimal**

### For Medium Datasets (100K-1M rows)
- **Polars Native** for maximum performance
- **ConnectorX methods** for good performance with flexibility

### For Large Datasets (>1M rows)
- **Polars Native strongly recommended** - 3-4x faster than alternatives
- **ConnectorX → Polars Direct** as second choice
- **Avoid traditional methods** unless required for compatibility

### Memory Considerations
- **ConnectorX → Pandas** most memory efficient for large datasets
- **Polars Native** good balance of speed and memory usage
- **Traditional methods** least memory efficient

## Technical Notes

- All tests performed on local SQL Server connection (no network latency)
- Results represent single-run measurements
- Memory measurements include peak usage during operation
- Dataset contains mixed data types typical of financial/trading data
- Performance characteristics may vary with different data types and query patterns
