import random
from datetime import timedelta

import pandas as pd
import pyodbc
from faker import Faker

from src.config.settings import (
    SQL_SERVER,
    SQL_DATABASE,
    SQL_USERNAME,
    SQL_PASSWORD
)


fake = Faker("en_GB")
random.seed(42)


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


def create_departments():
    return pd.DataFrame(
        [
            [1, "St Thomas Hospital", "A&E", "Emergency Medicine"],
            [2, "St Thomas Hospital", "Cardiology", "Heart"],
            [3, "St Thomas Hospital", "Orthopaedics", "Bones"],
            [4, "Royal London Hospital", "A&E", "Emergency Medicine"],
            [5, "Royal London Hospital", "Respiratory", "Lungs"],
            [6, "Royal London Hospital", "Neurology", "Brain"],
        ],
        columns=["department_id", "hospital_site", "department_name", "specialty"]
    )


def create_patients(number_of_patients=500):
    rows = []

    for patient_id in range(1, number_of_patients + 1):
        rows.append({
            "patient_id": patient_id,
            "nhs_number": str(fake.unique.random_number(digits=10)),
            "gender": random.choice(["Male", "Female"]),
            "date_of_birth": fake.date_of_birth(minimum_age=1, maximum_age=95),
            "postcode": fake.postcode()
        })

    return pd.DataFrame(rows)


def create_admissions(number_of_admissions=1200, number_of_patients=500):
    rows = []

    for admission_id in range(1, number_of_admissions + 1):
        admission_date = fake.date_between(start_date="-2y", end_date="today")

        if random.random() < 0.85:
            discharge_date = admission_date + timedelta(days=random.randint(1, 21))
        else:
            discharge_date = None

        rows.append({
            "admission_id": admission_id,
            "patient_id": random.randint(1, number_of_patients),
            "department_id": random.randint(1, 6),
            "admission_date": admission_date,
            "discharge_date": discharge_date,
            "admission_type": random.choice(["Emergency", "Elective", "Transfer"]),
            "diagnosis_code": random.choice(["I10", "J45", "E11", "S72", "N18", "F32"])
        })

    return pd.DataFrame(rows)


def create_appointments(number_of_appointments=2000, number_of_patients=500):
    rows = []

    for appointment_id in range(1, number_of_appointments + 1):
        rows.append({
            "appointment_id": appointment_id,
            "patient_id": random.randint(1, number_of_patients),
            "department_id": random.randint(1, 6),
            "appointment_date": fake.date_between(start_date="-1y", end_date="+6m"),
            "status": random.choice(["Completed", "Cancelled", "DNA", "Scheduled"]),
            "waiting_days": random.randint(1, 250)
        })

    return pd.DataFrame(rows)


def clear_existing_data(cursor):
    cursor.execute("DELETE FROM dbo.appointments")
    cursor.execute("DELETE FROM dbo.admissions")
    cursor.execute("DELETE FROM dbo.patients")
    cursor.execute("DELETE FROM dbo.departments")


def insert_dataframe(cursor, df, table_name, columns):
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)

    sql = f"""
    INSERT INTO dbo.{table_name} ({column_list})
    VALUES ({placeholders})
    """

    values = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    cursor.fast_executemany = True
    cursor.executemany(sql, values)

    print(f"Inserted {len(df)} rows into dbo.{table_name}")


departments = create_departments()
patients = create_patients()
admissions = create_admissions()
appointments = create_appointments()

conn = get_connection()
cursor = conn.cursor()

print("Clearing old data...")
clear_existing_data(cursor)

print("Loading new sample data...")

insert_dataframe(
    cursor,
    departments,
    "departments",
    ["department_id", "hospital_site", "department_name", "specialty"]
)

insert_dataframe(
    cursor,
    patients,
    "patients",
    ["patient_id", "nhs_number", "gender", "date_of_birth", "postcode"]
)

insert_dataframe(
    cursor,
    admissions,
    "admissions",
    [
        "admission_id",
        "patient_id",
        "department_id",
        "admission_date",
        "discharge_date",
        "admission_type",
        "diagnosis_code"
    ]
)

insert_dataframe(
    cursor,
    appointments,
    "appointments",
    [
        "appointment_id",
        "patient_id",
        "department_id",
        "appointment_date",
        "status",
        "waiting_days"
    ]
)

conn.commit()
conn.close()

print("Sample NHS-style data loaded into Azure SQL successfully.")