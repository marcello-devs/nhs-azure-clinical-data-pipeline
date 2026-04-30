DROP TABLE IF EXISTS dbo.appointments;
DROP TABLE IF EXISTS dbo.admissions;
DROP TABLE IF EXISTS dbo.patients;
DROP TABLE IF EXISTS dbo.departments;

CREATE TABLE dbo.departments (
    department_id INT NOT NULL PRIMARY KEY,
    hospital_site NVARCHAR(100) NOT NULL,
    department_name NVARCHAR(100) NOT NULL,
    specialty NVARCHAR(100) NOT NULL
);

CREATE TABLE dbo.patients (
    patient_id INT NOT NULL PRIMARY KEY,
    nhs_number NVARCHAR(20) NOT NULL,
    gender NVARCHAR(20) NOT NULL,
    date_of_birth DATE NOT NULL,
    postcode NVARCHAR(20) NULL
);

CREATE TABLE dbo.admissions (
    admission_id INT NOT NULL PRIMARY KEY,
    patient_id INT NOT NULL,
    department_id INT NOT NULL,
    admission_date DATE NOT NULL,
    discharge_date DATE NULL,
    admission_type NVARCHAR(50) NOT NULL,
    diagnosis_code NVARCHAR(20) NOT NULL
);

CREATE TABLE dbo.appointments (
    appointment_id INT NOT NULL PRIMARY KEY,
    patient_id INT NOT NULL,
    department_id INT NOT NULL,
    appointment_date DATE NOT NULL,
    status NVARCHAR(50) NOT NULL,
    waiting_days INT NOT NULL
);