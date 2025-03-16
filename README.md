import os
import pandas as pd
import mysql.connector
from datetime import datetime

# --- Helper Functions ---
def safe_str(val, max_len=None):
    """Return a stripped string; if val is NaN, return an empty string.
       Optionally, truncate to max_len characters."""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if max_len and len(s) > max_len:
        s = s[:max_len]
    return s

def safe_float(val):
    """Return a float value; if NaN or conversion fails, return 0.0."""
    try:
        if pd.isna(val):
            return 0.0
        return float(val)
    except:
        return 0.0

def safe_date(val):
    """Return a date value if not NaN; otherwise return None."""
    if pd.isna(val):
        return None
    return val  # Assumes Excel parsing converts date columns correctly

# --- Configuration ---
folder_path = r"D:\GANESH SONAWANE DATA\GANESH SONAWANE\KALYA EXPORTS - RODTEP RETURN FILING DATA"
file_name = "FOREX SHEET 2023-2024.xlsx"
file_path = os.path.join(folder_path, file_name)

# Define maximum allowed FOB_Value; adjust as required
MAX_FOB_VALUE = 999999999999999999.99

# --- Load Excel Data ---
df = pd.read_excel(file_path, sheet_name=0, engine="openpyxl")

# Debug: Print column names for verification
print("Columns in Excel:", df.columns.tolist())

# Clean up column names by stripping extra spaces
df.columns = df.columns.str.strip()

# --- Connect to MySQL ---
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ganesh@1234@",
        database="export_import_db"
    )
    cursor = conn.cursor()
    print("✅ MySQL connection successful!")
except mysql.connector.Error as err:
    print(f"❌ MySQL Connection Error: {err}")
    exit()

# --- Create Table (if not exists) ---
table_name = "forex_data"
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    SB_No VARCHAR(255),
    Inv_No VARCHAR(255),
    Inv_Date DATE,
    Customer VARCHAR(255),
    Country VARCHAR(255),
    Term VARCHAR(255),
    Port VARCHAR(255),
    Currency VARCHAR(50),
    Inv_Val DECIMAL(20,2),
    INR_VALUE DECIMAL(20,2),
    FOB_Value DECIMAL(20,2),
    Insurance DECIMAL(20,2),
    Freight DECIMAL(20,2),
    Net_Weight DECIMAL(20,2),
    RODTEP DECIMAL(20,2),
    DBK DECIMAL(20,2),
    BRC_Value DECIMAL(20,2),
    BRC_Diff DECIMAL(20,2),
    Status VARCHAR(255)
);
"""
cursor.execute(create_table_query)
print("✅ Table checked/created successfully!")

# --- Insert/Update Data ---
for index, row in df.iterrows():
    try:
        sb_no     = safe_str(row.get("SB No", ""))
        inv_no    = safe_str(row.get("Inv No", ""))
        inv_date  = safe_date(row.get("Inv Date", None))
        customer  = safe_str(row.get("Customer", ""))
        country   = safe_str(row.get("Country", ""))
        term      = safe_str(row.get("Term", ""))
        port      = safe_str(row.get("Port", ""))
        currency  = safe_str(row.get("Currency", ""), max_len=50)
        inv_val   = safe_float(row.get("Inv Val", 0))
        inr_value = safe_float(row.get("INR VALUE", 0))
        
        # Handle FOB_Value with an explicit conversion and clamping:
        # Check for trailing space in column name.
        raw_fob = row.get("FOB Value", None)
        if raw_fob is None:
            raw_fob = row.get("FOB Value ", 0)
        try:
            fob_val = float(raw_fob) if not pd.isna(raw_fob) else 0.0
        except Exception as e:
            fob_val = 0.0
        if fob_val > MAX_FOB_VALUE:
            print(f"⚠️ Row {index}: FOB_Value {fob_val} exceeds maximum, clamping to {MAX_FOB_VALUE}.")
            fob_val = MAX_FOB_VALUE
        fob_value = fob_val
        
        insurance = safe_float(row.get("Insurance", 0))
        freight   = safe_float(row.get("Fright", 0))
        net_weight = safe_float(row.get("NET WT", 0))
        rodtep    = safe_float(row.get("RODTEP ALLOWED WHICH EVER IS MINIMUM", 0))
        dbk       = safe_float(row.get("DBK 0.15% ON SB FOB VALUE", 0))
        brc_value = safe_float(row.get("BRC Values/ IRM Utilize Value", 0))
        brc_diff  = safe_float(row.get("BRC DIFFERENCE", 0))
        status    = safe_str(row.get("REMARK", ""))
        
        insert_query = f"""
        INSERT INTO {table_name} (
            SB_No, Inv_No, Inv_Date, Customer, Country, Term, Port, Currency, 
            Inv_Val, INR_VALUE, FOB_Value, Insurance, Freight, Net_Weight, 
            RODTEP, DBK, BRC_Value, BRC_Diff, Status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            Inv_Date = VALUES(Inv_Date), 
            Customer = VALUES(Customer), 
            Country = VALUES(Country), 
            Term = VALUES(Term), 
            Port = VALUES(Port);
        """
        cursor.execute(insert_query, (
            sb_no, inv_no, inv_date, customer, country, term, port, currency,
            inv_val, inr_value, fob_value, insurance, freight, net_weight,
            rodtep, dbk, brc_value, brc_diff, status
        ))
    except mysql.connector.Error as err:
        print(f"❌ Error inserting row {index}: {err}")

conn.commit()
cursor.close()
conn.close()
print("✅ Data updated successfully in MySQL!")

# --- Generate MIS Report ---
report_file = os.path.join(folder_path, f"Forex_Report_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx")
df.to_excel(report_file, index=False)
print(f"✅ MIS Report generated: {report_file}")
