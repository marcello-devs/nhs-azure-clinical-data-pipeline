from pathlib import Path
import pandas as pd


LOCAL_EXTRACT_PATH = Path("data/local_extracts")
REPORT_PATH = Path("data/validation_reports")
REPORT_PATH.mkdir(parents=True, exist_ok=True)


tables = [
    "departments",
    "patients",
    "admissions",
    "appointments"
]


validation_results = []

for table in tables:
    file_path = LOCAL_EXTRACT_PATH / f"{table}.csv"

    print(f"Validating {file_path}...")

    df = pd.read_csv(file_path)

    result = {
        "table_name": table,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_rows": df.duplicated().sum(),
        "total_null_values": df.isna().sum().sum()
    }

    for column in df.columns:
        result[f"nulls_{column}"] = df[column].isna().sum()

    validation_results.append(result)


report_df = pd.DataFrame(validation_results)

report_file = REPORT_PATH / "raw_extract_validation_report.csv"
report_df.to_csv(report_file, index=False)

print("Validation report created:")
print(report_file)

print("\nValidation summary:")
print(report_df)