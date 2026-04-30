/* 1. Admission metrics by hospital and department */

WITH admission_base AS (
    SELECT
        a.admission_id,
        a.patient_id,
        a.department_id,
        d.hospital_site,
        d.department_name,
        a.admission_date,
        a.discharge_date,
        DATEDIFF(DAY, a.admission_date, a.discharge_date) AS length_of_stay_days,
        CASE
            WHEN a.discharge_date IS NULL THEN 1
            ELSE 0
        END AS is_active_admission
    FROM dbo.admissions a
    INNER JOIN dbo.departments d
        ON a.department_id = d.department_id
)
SELECT
    hospital_site,
    department_name,
    COUNT(*) AS total_admissions,
    AVG(CAST(length_of_stay_days AS FLOAT)) AS avg_length_of_stay_days,
    SUM(is_active_admission) AS active_admissions
FROM admission_base
GROUP BY
    hospital_site,
    department_name
ORDER BY
    hospital_site,
    department_name;


/* 2. Waiting list pressure by department */

WITH waiting_base AS (
    SELECT
        ap.appointment_id,
        ap.patient_id,
        ap.department_id,
        d.hospital_site,
        d.department_name,
        ap.appointment_date,
        ap.status,
        ap.waiting_days,
        CASE
            WHEN ap.waiting_days > 126 THEN 1
            ELSE 0
        END AS over_18_weeks
    FROM dbo.appointments ap
    INNER JOIN dbo.departments d
        ON ap.department_id = d.department_id
)
SELECT
    hospital_site,
    department_name,
    COUNT(*) AS total_appointments,
    SUM(over_18_weeks) AS patients_over_18_weeks,
    AVG(CAST(waiting_days AS FLOAT)) AS avg_waiting_days,
    MAX(waiting_days) AS max_waiting_days
FROM waiting_base
GROUP BY
    hospital_site,
    department_name
ORDER BY
    patients_over_18_weeks DESC;


/* 3. Patient admission ranking using window functions */

WITH patient_admissions AS (
    SELECT
        a.patient_id,
        p.nhs_number,
        a.admission_id,
        a.admission_date,
        a.discharge_date,
        d.hospital_site,
        d.department_name,
        ROW_NUMBER() OVER (
            PARTITION BY a.patient_id
            ORDER BY a.admission_date
        ) AS admission_sequence,
        LAG(a.discharge_date) OVER (
            PARTITION BY a.patient_id
            ORDER BY a.admission_date
        ) AS previous_discharge_date
    FROM dbo.admissions a
    INNER JOIN dbo.patients p
        ON a.patient_id = p.patient_id
    INNER JOIN dbo.departments d
        ON a.department_id = d.department_id
)
SELECT
    patient_id,
    nhs_number,
    admission_id,
    hospital_site,
    department_name,
    admission_date,
    discharge_date,
    admission_sequence,
    previous_discharge_date,
    DATEDIFF(DAY, previous_discharge_date, admission_date) AS days_since_previous_discharge
FROM patient_admissions
ORDER BY
    patient_id,
    admission_sequence;


/* 4. Simulated 30-day readmission flag */

WITH patient_admissions AS (
    SELECT
        a.patient_id,
        a.admission_id,
        a.admission_date,
        a.discharge_date,
        d.hospital_site,
        d.department_name,
        LAG(a.discharge_date) OVER (
            PARTITION BY a.patient_id
            ORDER BY a.admission_date
        ) AS previous_discharge_date
    FROM dbo.admissions a
    INNER JOIN dbo.departments d
        ON a.department_id = d.department_id
),
readmission_flags AS (
    SELECT
        *,
        CASE
            WHEN previous_discharge_date IS NOT NULL
             AND DATEDIFF(DAY, previous_discharge_date, admission_date) BETWEEN 0 AND 30
            THEN 1
            ELSE 0
        END AS is_30_day_readmission
    FROM patient_admissions
)
SELECT
    hospital_site,
    department_name,
    COUNT(*) AS total_admissions,
    SUM(is_30_day_readmission) AS readmissions_30_day,
    SUM(is_30_day_readmission) * 100.0 / COUNT(*) AS readmission_rate_percent
FROM readmission_flags
GROUP BY
    hospital_site,
    department_name
ORDER BY
    readmission_rate_percent DESC;


/* 5. Department pressure score */

WITH admissions_summary AS (
    SELECT
        department_id,
        COUNT(*) AS total_admissions,
        SUM(CASE WHEN discharge_date IS NULL THEN 1 ELSE 0 END) AS active_admissions
    FROM dbo.admissions
    GROUP BY department_id
),
waiting_summary AS (
    SELECT
        department_id,
        COUNT(*) AS total_appointments,
        SUM(CASE WHEN waiting_days > 126 THEN 1 ELSE 0 END) AS over_18_week_waits,
        AVG(CAST(waiting_days AS FLOAT)) AS avg_waiting_days
    FROM dbo.appointments
    GROUP BY department_id
)
SELECT
    d.hospital_site,
    d.department_name,
    a.total_admissions,
    a.active_admissions,
    w.total_appointments,
    w.over_18_week_waits,
    w.avg_waiting_days,
    (
        (a.active_admissions * 2)
        + (w.over_18_week_waits * 1.5)
        + (w.avg_waiting_days / 10)
    ) AS department_pressure_score
FROM dbo.departments d
LEFT JOIN admissions_summary a
    ON d.department_id = a.department_id
LEFT JOIN waiting_summary w
    ON d.department_id = w.department_id
ORDER BY
    department_pressure_score DESC;