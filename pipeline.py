import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv
 
load_dotenv()
 


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

csv_path = os.path.join(BASE_DIR, "source_data.csv")
excel_path = os.path.join(BASE_DIR, "source_data2.xlsx")

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

if not os.path.exists(excel_path):
    raise FileNotFoundError(f"Excel file not found: {excel_path}")

# Part-1:Extraction &  Raw Layer

df_csv = pd.read_csv(csv_path)
df_excel = pd.read_excel(excel_path)

df_csv["SOURCE"] = "CSVFILE"
df_excel["SOURCE"] = "EXCELFILE"

df_raw = pd.concat([df_csv, df_excel], ignore_index=True)

gender_map = {
    "male": "M",
    "m": "M",
    "female": "F",
    "f": "F"
}

df_raw["GENDER"] = (
    df_raw["GENDER"]
    .astype(str)
    .str.lower()
    .map(gender_map)
    .fillna("O")
)

df_raw["DOB"] = pd.to_datetime(df_raw["DOB"], errors="coerce")
df_raw["DOB"] = df_raw["DOB"].dt.strftime("%d-%m-%Y")

df_raw["LOAD_TIMESTAMP"] = pd.Timestamp.now()

df_raw = df_raw[
    ["USER_ID", "NAME", "GENDER", "DOB", "SOURCE", "LOAD_TIMESTAMP"]
].reset_index(drop=True)  


# Part-2: Final Layer

csv_df = df_raw[df_raw["SOURCE"] == "CSVFILE"]
excel_df = df_raw[df_raw["SOURCE"] == "EXCELFILE"]

final_df = csv_df.merge(
    excel_df,
    on="USER_ID",
    how="inner",
    suffixes=("_CSV", "_EXCEL")
)

final_df["DOB"] = pd.to_datetime(
    final_df["DOB_CSV"],
    format="%d-%m-%Y",
    errors="coerce"
)

today = pd.to_datetime("today")
final_df["AGE"] = (today - final_df["DOB"]).dt.days // 365

final_df = final_df[final_df["AGE"] > 18]

final_df = final_df[
    ["USER_ID", "NAME_EXCEL", "GENDER_EXCEL", "DOB_EXCEL", "AGE"]
]

final_df.columns = ["USER_ID", "NAME", "GENDER", "DOB", "AGE"]

final_df = final_df.reset_index(drop=True)   


# Part-3: Load into SnowflakeE

conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"))

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS RAW_USER_DATA")
cursor.execute("DROP TABLE IF EXISTS FINAL_USER_DATA")

write_pandas(
    conn,
    df_raw,
    table_name="RAW_USER_DATA",
    auto_create_table=True
)

write_pandas(
    conn,
    final_df,
    table_name="FINAL_USER_DATA",
    auto_create_table=True
)

conn.close()

print("Done")
