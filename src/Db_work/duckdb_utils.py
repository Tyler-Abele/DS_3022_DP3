import duckdb
from pathlib import Path
from typing import List, Optional


def inspect_database(db_path: str, read_only: bool = True) -> None:
    """
    Inspect a DuckDB database and print information about tables and schemas.
    
    Args:
        db_path: Path to the DuckDB database file
        read_only: Whether to open in read-only mode
    """
    # Convert to absolute path
    db_path = str(Path(db_path).resolve())
    
    try:
        with duckdb.connect(db_path, read_only=read_only) as con:
            # Get all tables
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            
            print(f"\nDatabase: {db_path}")
            print(f"Number of tables: {len(tables)}\n")
            
            for table in tables:
                table_name = table[0]
                print(f"Table: {table_name}")
                
                # Get row count
                row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"  Rows: {row_count:,}")
                
                # Get column info
                columns = con.execute(
                    f"PRAGMA table_info('{table_name}')"
                ).fetchall()
                
                print(f"  Columns ({len(columns)}):")
                for col in columns:
                    col_name, col_type = col[1], col[2]
                    print(f"    - {col_name}: {col_type}")
                print()
                
    except Exception as e:
        print(f"Error inspecting database: {e}")


def list_tables(db_path: str) -> List[str]:
    """
    List all tables in a DuckDB database.
    
    Args:
        db_path: Path to the DuckDB database file
        
    Returns:
        List of table names
    """
    db_path = str(Path(db_path).resolve())
    
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            return [t[0] for t in tables]
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []


def query_table(db_path: str, table_name: str, limit: int = 10) -> Optional[List]:
    """
    Query a table and return sample rows.
    
    Args:
        db_path: Path to the DuckDB database file
        table_name: Name of the table to query
        limit: Number of rows to return
        
    Returns:
        List of rows (as tuples)
    """
    db_path = str(Path(db_path).resolve())
    
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            result = con.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchall()
            return result
    except Exception as e:
        print(f"Error querying table: {e}")
        return None


if __name__ == "__main__":
    from pathlib import Path
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    db_path = PROJECT_ROOT / "air_ops.duckdb"
    
    inspect_database(str(db_path))
    