# High-Performance Python SQL Server benchmark

This project benchmarks the various python connection methods for SQL Server. 

<details>
<summary>🧬 <strong>Spoiler Alert:</strong></summary>

**Polars Native** is the clear winner, with **ConnectorX → Polars Direct** and **ConnectorX → Arrow → Polars** methods closely following.

**pyodbc → Pandas** and **SQLAlchemy → Pandas** methods are the slowest, but still very fast.

**ConnectorX → Pandas** method is the fastest among the traditional methods, but still 2-4x slower than Polars.

There still is the matter of how you want to work your data. The conversion to pandas or arrow is still going to be a factor in how you use the data.

</details>



📊 **[View Benchmark Results](results.md)** - Complete performance analysis across dataset sizes

## 🚀 Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   Create a `.env` file with your SQL Server connection details:
   ```env
   MSSQL_SERVER=your_server
   MSSQL_DB=your_database
   MSSQL_USER=your_username
   MSSQL_PWD=your_password
   MSSQL_PORT=1433
   MSSQL_DRIVER=ODBC Driver 17 for SQL Server


   SQL_BENCHMARK_TABLE=your_benchmark_table
   ```

3. **Run the examples:**
   ```bash
   python main.py
   ```

## 🛠️ Dependencies

- **connectorx**: Rust-based database connectivity
- **polars**: High-performance DataFrame library
- **pyarrow**: Apache Arrow Python bindings
- **arrow-odbc**: ODBC connectivity for Arrow
- **pandas**: Traditional DataFrame library (for comparison)
- **python-dotenv**: Environment variable management

## 📚 Further Reading

- [ConnectorX Documentation](https://sfu-db.github.io/connector-x/)
- [Polars User Guide](https://pola-rs.github.io/polars/)
- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
- [Performance comparison benchmarks](https://github.com/sfu-db/connector-x#performance)
