import pyodbc
from src.config.settings import (
    SQL_SERVER,
    SQL_DATABASE,
    SQL_USERNAME,
    SQL_PASSWORD
)

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

print("Attempting connection...")

conn = pyodbc.connect(connection_string)

print("Connection successful!")

cursor = conn.cursor()

cursor.execute("SELECT GETDATE() AS server_time")

row = cursor.fetchone()

print("Database time:", row.server_time)

conn.close()
print("Connection closed.")