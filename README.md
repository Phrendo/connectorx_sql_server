# High-Performance SQL Data Access with ConnectorX and Arrow

This project benchmarks the various python connection methods for SQL Server. 

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
