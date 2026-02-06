
# SourceFiles_etl

Simple ETL (Extract, Transform, Load) project written in Python. This pipeline extracts and processes data from source files, applies transformation rules, and loads it into a destination. 

## Overview

This repository contains:

* **.gitignore** – Git ignore rules. 
* **README.md** – (this document). 
* **pipeline.py** – Main Python script that runs the ETL pipeline. 
* **requirements.txt** – List of required Python packages. 
* **source_data.csv** – Sample source CSV data. 
* **source_data2.xlsx** – Sample source Excel data.

# Architecture
Source Files
1. Extraction (Pandas)
2. Raw Layer (Cleansing Data)
3. Final Layer (Join + Business Rules)
4. Snowflake (RAW_USER_DATA & FINAL_USER_DATA)


## Pipeline Description

The ETL pipeline performs the following steps: 

1. **Extract**

   * Read data from multiple sources including `.csv` and `.xlsx` files. 

2. **Transform**

   * Clean and standardize the raw data to ensure consistency and accuracy. 
   * Apply business rules and remove or correct any invalid entries to form a meaningful final dataset.
  
  1) part-1 
   * Standardizes gender values (M / F / O) for consistency
   * Converts DOB to proper date format
   * Converts ACCOUNT_CREATED to timestamp format
   * Adds LOAD_TIMESTAMP to track when data was loaded
   
  2) part-2
   * Inner join on USER_ID to include only matching records from both files
   * Calculates age from DOB
   * Filters users who are older than 18 years
   * Outputs clean business-ready dataset

3. **Load**

   * Load the processed dataset into the target destination (e.g., a database system such as Snowflake using `write_pandas` for bulk loading).
   * Uses write_pandas() for efficient bulk loading into Snowflake
   * Avoids row-by-row inserts to improve speed
   * Uses vectorized Pandas operations instead of loops for faster transformations

> *Note:* The detailed implementation logic (Python code and how each step is executed) can be found inside `pipeline.py`. 

## Installation

To run this project locally:

1. **Clone the repository:**

   ```bash
   git clone https://github.com/KOTTAPALLISRAVYA2004/SourceFiles_etl.git
   cd SourceFiles_etl
   ```

2. **Install required Python packages:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the pipeline:**

   ```bash
   python pipeline.py
   ```

# Environment Configuration
Create a .env file in the root directory:

SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema


## Usage

* Place your source data files (CSV, Excel) into the repository folder.
* Update the pipeline script to reference your file paths.
* Customize transformation logic as needed for your ETL use case.

# Validation(to check the data)
* SELECT * FROM RAW_USER_DATA;
* SELECT * FROM FINAL_USER_DATA;


## Notes

* This README file gives a high‑level overview of the project. For deeper code explanations, open **pipeline.py**. 
* Ensure Python (3.x) is installed and environment dependencies are met before running the pipeline.

# Author
## KOTTAPALLI SRAVYA
Data Engineering Project


---

