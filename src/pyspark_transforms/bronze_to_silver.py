from pathlib import Path
import shutil
import pandas as pd


BRONZE_PATH = Path("data/bronze")
SILVER_PATH = Path("data/silver")


def write_silver(df, table_name):
    output_folder = SILVER_PATH / table_name

    if output_folder.exists():
        shutil.rmtree(output_folder)

    output_folder.mkdir(parents=True, exist_ok=True)

    output_file = output_folder / f"{table_name}.parquet"
    df.to_parquet(output_file, index=False)

    print(f"Written Silver dataset: {output_file}")


departments = pd.read_parquet(BRONZE_PATH / "departments" / "departments.parquet")
patients = pd.read_parquet(BRONZE_PATH / "patients" / "patients.parquet")
admissions = pd.read_parquet(BRONZE_PATH / "admissions" / "admissions.parquet")
appointments = pd.read_parquet(BRONZE_PATH / "appointments" / "appointments.parquet")


patients = patients.drop_duplicates(subset=["nhs_number"])
patients["date_of_birth"] = pd.to_datetime(patients["date_of_birth"])
patients["age"] = (
    (pd.Timestamp.today() - patients["date_of_birth"]).dt.days / 365.25
).astype(int)


admissions["admission_date"] = pd.to_datetime(admissions["admission_date"])
admissions["discharge_date"] = pd.to_datetime(admissions["discharge_date"])

admissions["length_of_stay_days"] = (
    admissions["discharge_date"] - admissions["admission_date"]
).dt.days


appointments["appointment_date"] = pd.to_datetime(appointments["appointment_date"])
appointments["over_18_weeks"] = appointments["waiting_days"] > 126


write_silver(departments, "departments")
write_silver(patients, "patients")
write_silver(admissions, "admissions")
write_silver(appointments, "appointments")

print("Silver layer complete.")