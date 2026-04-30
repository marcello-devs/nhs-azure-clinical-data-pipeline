from pathlib import Path
import shutil
import pandas as pd


SILVER_PATH = Path("data/silver")
GOLD_PATH = Path("data/gold")


def write_gold(df, table_name):
    output_folder = GOLD_PATH / table_name

    if output_folder.exists():
        shutil.rmtree(output_folder)

    output_folder.mkdir(parents=True, exist_ok=True)

    output_file = output_folder / f"{table_name}.parquet"
    df.to_parquet(output_file, index=False)

    print(f"Written Gold dataset: {output_file}")


departments = pd.read_parquet(SILVER_PATH / "departments" / "departments.parquet")
patients = pd.read_parquet(SILVER_PATH / "patients" / "patients.parquet")
admissions = pd.read_parquet(SILVER_PATH / "admissions" / "admissions.parquet")
appointments = pd.read_parquet(SILVER_PATH / "appointments" / "appointments.parquet")


admissions_enriched = admissions.merge(
    departments,
    on="department_id",
    how="left"
)

appointments_enriched = appointments.merge(
    departments,
    on="department_id",
    how="left"
)


admission_kpis = (
    admissions_enriched
    .groupby(["hospital_site", "department_name"])
    .agg(
        total_admissions=("admission_id", "count"),
        avg_length_of_stay_days=("length_of_stay_days", "mean"),
        active_admissions=("discharge_date", lambda x: x.isna().sum())
    )
    .reset_index()
)


waiting_list_kpis = (
    appointments_enriched
    .groupby(["hospital_site", "department_name"])
    .agg(
        total_appointments=("appointment_id", "count"),
        patients_over_18_weeks=("over_18_weeks", "sum"),
        avg_waiting_days=("waiting_days", "mean")
    )
    .reset_index()
)


patient_demographics = (
    patients
    .assign(
        age_band=pd.cut(
            patients["age"],
            bins=[0, 18, 40, 65, 120],
            labels=["0-18", "19-40", "41-65", "66+"]
        )
    )
    .groupby(["gender", "age_band"], observed=False)
    .agg(
        patient_count=("patient_id", "count")
    )
    .reset_index()
)


write_gold(admission_kpis, "admission_kpis")
write_gold(waiting_list_kpis, "waiting_list_kpis")
write_gold(patient_demographics, "patient_demographics")

print("Gold layer complete.")