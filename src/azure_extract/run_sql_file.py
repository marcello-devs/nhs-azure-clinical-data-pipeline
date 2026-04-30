from pathlib import Path
import pyodbc

from src.config.settings import (
    SQL_SERVER,
    SQL_DATABASE,
    SQL_USERNAME,
    SQL_PASSWORD
)


def get_connection():
    connection_string = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER={SQL_SERVER};
    DATABASE={SQL_DATABASE};
    UID={SQL_USERNAME};
    PWD={SQL_PASSWORD};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
    """
    return pyodbc.connect(connection_string)


sql_file = Path("src/sql/advanced_nhs_kpis.sql")
sql_text = sql_file.read_text()

statements = [
    statement.strip()
    for statement in sql_text.split(";")
    if statement.strip()
]

conn = get_connection()
cursor = conn.cursor()

for statement_number, statement in enumerate(statements, start=1):
    print(f"\nRunning SQL statement {statement_number}...")
    cursor.execute(statement)

    if cursor.description:
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchmany(10)

        print("Columns:", columns)

        for row in rows:
            print(tuple(row))

conn.close()

print("\nAdvanced KPI SQL script completed.")