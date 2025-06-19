#!/usr/bin/env python3
"""
Systematic benchmark script for SQL Server data access methods.
Configurable via benchmark_config.yaml, outputs results to CSV.
"""

import os
import time
import psutil
import gc
import warnings
import csv
import platform
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv
import yaml
import pandas as pd
import polars as pl
import connectorx as cx
import pyodbc
from sqlalchemy import create_engine, text

# Suppress known warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="connectorx")
warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")

def load_config():
    """Load configuration from YAML file."""
    with open('benchmark_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Load environment variables
    load_dotenv()
    
    # Replace environment variable placeholders
    table_name = os.getenv("SQL_BENCHMARK_TABLE")
    if not table_name:
        print("Error: SQL_BENCHMARK_TABLE not found in .env file")
        exit(1)
    
    config['database']['table_name'] = table_name
    return config

def get_connection_strings():
    """Build connection strings from environment variables."""
    load_dotenv()
    SERVER   = os.getenv("MSSQL_SERVER")
    DATABASE = os.getenv("MSSQL_DB")
    USER     = os.getenv("MSSQL_USER")
    PASSWORD = os.getenv("MSSQL_PWD")
    PORT     = os.getenv("MSSQL_PORT", "1433")
    DRIVER   = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
    
    conn_uri_cx = (
        f"mssql://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}"
        f"?driver={DRIVER.replace(' ', '+')}"
    )
    
    conn_odbc = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER},{PORT};"
        f"DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        "Trusted_Connection=no;MARS_Connection=yes"
    )
    
    conn_sqlalchemy = (
        f"mssql+pyodbc://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}"
        f"?driver={DRIVER.replace(' ', '+')}&TrustServerCertificate=yes"
    )
    
    return conn_uri_cx, conn_odbc, conn_sqlalchemy

def get_test_query(table_name: str, row_count: int, offset: int = 0) -> str:
    """Generate test query for specified row count and offset."""
    return f"""
    SELECT *
    FROM {table_name}
    ORDER BY (SELECT NULL)
    OFFSET {offset} ROWS
    FETCH NEXT {row_count} ROWS ONLY
    """

class PerformanceMonitor:
    """Monitor memory and time for each test run."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.start_memory = None
        
    def start(self):
        gc.collect()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.start_time = time.perf_counter()
        
    def stop(self) -> Dict[str, float]:
        end_time = time.perf_counter()
        end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        
        return {
            'duration_seconds': end_time - self.start_time,
            'memory_peak_mb': end_memory,
            'memory_delta_mb': end_memory - self.start_memory
        }

def test_connectorx_pandas(query: str, conn_uri: str) -> Dict[str, Any]:
    """ConnectorX to Pandas DataFrame"""
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        df = cx.read_sql(conn_uri, query, return_type="pandas")
        result = monitor.stop()
        result['rows'] = len(df)
        result['success'] = True
        result['error'] = None
        del df
        return result
    except Exception as e:
        result = monitor.stop()
        result['rows'] = 0
        result['success'] = False
        result['error'] = str(e)
        return result

def test_polars_native(query: str, conn_odbc: str) -> Dict[str, Any]:
    """Polars native database reader"""
    monitor = PerformanceMonitor()
    monitor.start()

    try:
        df = pl.read_database(query, conn_odbc)
        result = monitor.stop()
        result['rows'] = len(df)
        result['success'] = True
        result['error'] = None
        del df
        return result
    except Exception as e:
        result = monitor.stop()
        result['rows'] = 0
        result['success'] = False
        # Get more detailed error info
        import traceback
        error_detail = traceback.format_exc()
        result['error'] = f"{type(e).__name__}: {str(e)} | Full trace: {error_detail[-200:]}"
        return result

def test_connectorx_arrow_polars(query: str, conn_uri: str) -> Dict[str, Any]:
    """ConnectorX to Arrow to Polars"""
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        arrow_table = cx.read_sql(conn_uri, query, return_type="arrow")
        df = pl.from_arrow(arrow_table)
        result = monitor.stop()
        result['rows'] = len(df)
        result['success'] = True
        result['error'] = None
        del arrow_table, df
        return result
    except Exception as e:
        result = monitor.stop()
        result['rows'] = 0
        result['success'] = False
        result['error'] = str(e)
        return result

def test_connectorx_polars_direct(query: str, conn_uri: str) -> Dict[str, Any]:
    """ConnectorX direct to Polars"""
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        df = cx.read_sql(conn_uri, query, return_type="polars")
        result = monitor.stop()
        result['rows'] = len(df)
        result['success'] = True
        result['error'] = None
        del df
        return result
    except Exception as e:
        result = monitor.stop()
        result['rows'] = 0
        result['success'] = False
        result['error'] = str(e)
        return result

def test_pyodbc_pandas(query: str, conn_odbc: str) -> Dict[str, Any]:
    """pyodbc to Pandas DataFrame"""
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        conn = pyodbc.connect(conn_odbc)
        df = pd.read_sql(query, conn)
        conn.close()
        result = monitor.stop()
        result['rows'] = len(df)
        result['success'] = True
        result['error'] = None
        del df
        return result
    except Exception as e:
        result = monitor.stop()
        result['rows'] = 0
        result['success'] = False
        result['error'] = str(e)
        return result

def test_sqlalchemy_pandas(query: str, conn_sqlalchemy: str) -> Dict[str, Any]:
    """SQLAlchemy to Pandas DataFrame"""
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        engine = create_engine(conn_sqlalchemy)
        df = pd.read_sql(text(query), engine)
        engine.dispose()
        result = monitor.stop()
        result['rows'] = len(df)
        result['success'] = True
        result['error'] = None
        del df
        return result
    except Exception as e:
        result = monitor.stop()
        result['rows'] = 0
        result['success'] = False
        result['error'] = str(e)
        return result

def get_system_info():
    """Collect system information for the benchmark."""
    # Try to determine if we're running locally on the SQL Server
    server_host = os.getenv("MSSQL_SERVER", "unknown")
    is_local = server_host.lower() in ['localhost', '127.0.0.1', '.', '(local)']

    return {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'platform': platform.platform(),
        'python_version': platform.python_version(),
        'cpu_count': psutil.cpu_count(),
        'memory_gb': round(psutil.virtual_memory().total / (1024**3), 1),
        'sql_server_host': server_host,
        'network_context': 'local' if is_local else 'remote'
    }

def run_systematic_benchmark():
    """Run the complete systematic benchmark suite."""
    
    config = load_config()
    conn_uri_cx, conn_odbc, conn_sqlalchemy = get_connection_strings()
    
    # Method mapping
    method_functions = {
        'test_connectorx_pandas': lambda q: test_connectorx_pandas(q, conn_uri_cx),
        'test_polars_native': lambda q: test_polars_native(q, conn_odbc),
        'test_connectorx_arrow_polars': lambda q: test_connectorx_arrow_polars(q, conn_uri_cx),
        'test_connectorx_polars_direct': lambda q: test_connectorx_polars_direct(q, conn_uri_cx),
        'test_pyodbc_pandas': lambda q: test_pyodbc_pandas(q, conn_odbc),
        'test_sqlalchemy_pandas': lambda q: test_sqlalchemy_pandas(q, conn_sqlalchemy)
    }
    
    # Prepare CSV output
    csv_file = config['output']['csv_file']
    system_info = get_system_info()
    
    results = []
    
    print("Starting systematic benchmark...")
    print(f"Output file: {csv_file}")
    
    # Run all test scenarios
    for scenario in config['test_scenarios']:
        print(f"\nTesting scenario: {scenario['name']} ({scenario['row_count']:,} rows)")
        
        for method_config in config['methods']:
            method_name = method_config['name']
            method_func = method_functions[method_config['function']]
            
            print(f"  Method: {method_name}")
            
            # Run multiple times for this scenario/method combination
            for run in range(scenario['runs']):
                offset = run * scenario['row_count']
                query = get_test_query(config['database']['table_name'], 
                                     scenario['row_count'], offset)
                
                print(f"    Run {run + 1}/{scenario['runs']}...", end=" ")
                
                result = method_func(query)
                
                # Record result
                record = {
                    'timestamp': system_info['timestamp'],
                    'scenario': scenario['name'],
                    'method': method_name,
                    'row_count': scenario['row_count'],
                    'run_number': run + 1,
                    'duration_seconds': result['duration_seconds'],
                    'memory_peak_mb': result['memory_peak_mb'],
                    'memory_delta_mb': result['memory_delta_mb'],
                    'rows_returned': result['rows'],
                    'success': result['success'],
                    'error': result['error'],
                    'platform': system_info['platform'],
                    'python_version': system_info['python_version'],
                    'cpu_count': system_info['cpu_count'],
                    'memory_gb': system_info['memory_gb'],
                    'sql_server_host': system_info['sql_server_host'],
                    'network_context': system_info['network_context']
                }
                
                results.append(record)
                
                if result['success']:
                    print(f"{result['duration_seconds']:.2f}s")
                else:
                    print(f"FAILED: {result['error']}")
    
    # Write results to CSV
    if results:
        fieldnames = results[0].keys()
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\nResults written to {csv_file}")
        print(f"Total test runs: {len(results)}")
    
    return results

if __name__ == "__main__":
    print("Systematic SQL Server Performance Benchmark")
    print("Configuration-driven testing with CSV output")
    
    try:
        results = run_systematic_benchmark()
        print("\nBenchmark complete!")
    except Exception as e:
        print(f"Benchmark failed: {e}")
        exit(1)
