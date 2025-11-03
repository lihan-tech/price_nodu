# %%


# %%
import boto3
import os

# --- Read credentials from environment variables ---
ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

# --- Bucket and file info ---
BUCKET_NAME = "lihan"
OBJECT_KEY = "Desktop/Excel file/Symbols 2022.xlsx"
DOWNLOAD_FOLDER = r"C:\Users\bomma\Desktop\New folder"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
LOCAL_PATH = os.path.join(DOWNLOAD_FOLDER, os.path.basename(OBJECT_KEY))

# --- Connect to Cloudflare R2 ---
r2 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY
)

try:
    r2.download_file(BUCKET_NAME, OBJECT_KEY, LOCAL_PATH)
    print(f"✅ File downloaded successfully to: {LOCAL_PATH}")
except Exception as e:
    print("❌ Download failed:", e)


# %%
from fyers_apiv3 import fyersModel
from datetime import datetime
import pandas as pd
import pytz
import os
import time
import requests
import sys

# ----------------------------
# 1) Read Fyers credentials from GitHub Secrets
# ----------------------------
access_token = os.getenv("FYERS_ACCESS_TOKEN")
client_id = os.getenv("FYERS_CLIENT_ID")

if not access_token or not client_id:
    print("❌ Missing FYERS_ACCESS_TOKEN or FYERS_CLIENT_ID environment variables.")
    sys.exit(1)

# ----------------------------
# 2) File and folder paths
# ----------------------------
excel_file_path = r'C:\Users\bomma\Desktop\Daily Reports\Price\Stocks symbol\Symbols 2022.xlsx'
output_folder_path = r'C:\Users\bomma\Desktop\Daily Reports\Price\Day wise'
no_symbols_file_path = r'C:\Users\bomma\Desktop\Daily Reports\Price\No download\No symbols.xlsx'
error_log_file_path = r'C:\Users\bomma\Desktop\Daily Reports\Price\error\Error log.xlsx'
log_folder_path = r'C:\Users\bomma\Desktop\Daily Reports\Price'

# Ensure folders exist
os.makedirs(output_folder_path, exist_ok=True)
os.makedirs(os.path.dirname(no_symbols_file_path), exist_ok=True)
os.makedirs(os.path.dirname(error_log_file_path), exist_ok=True)

# ----------------------------
# 3) Setup Fyers client
# ----------------------------
fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, log_path=log_folder_path)
ist = pytz.timezone('Asia/Kolkata')

# ----------------------------
# 4) Helper functions
# ----------------------------
def is_internet_available():
    """Check internet connectivity."""
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except (requests.ConnectionError, requests.Timeout):
        return False

def wait_for_internet():
    """Wait until internet is available."""
    while not is_internet_available():
        print("No internet connection. Waiting...")
        time.sleep(5)

def fetch_stock_data(symbol):
    """Fetch 5-min candle data for symbol."""
    max_retries = 7
    range_from = "2025-09-12"
    range_to = "2025-09-19"
    
    for attempt in range(max_retries):
        try:
            data = {
                "symbol": f"NSE:{symbol}",
                "resolution": "5",
                "date_format": "1",
                "range_from": range_from,
                "range_to": range_to,
                "cont_flag": "1"
            }
            response = fyers.history(data=data)
            if 'candles' not in response:
                print(f"No candles for {symbol}: {response}")
                return pd.DataFrame(), False

            candles = [
                [datetime.utcfromtimestamp(c[0]).replace(tzinfo=pytz.utc).astimezone(ist).strftime('%Y-%m-%d %H:%M:%S'),
                 c[1], c[2], c[3], c[4], c[5]]
                for c in response['candles']
            ]
            return pd.DataFrame(candles, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']), True

        except Exception as e:
            wait_time = 2 ** (attempt + 1)
            print(f"Error fetching {symbol}: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)

    return pd.DataFrame(), False

# ----------------------------
# 5) Load symbol list
# ----------------------------
try:
    symbols_df = pd.read_excel(excel_file_path, sheet_name='Rama')
    if 'Symbols' not in symbols_df.columns:
        print("❌ Excel missing 'Symbols' column.")
        symbols_df = None
except Exception as e:
    print(f"❌ Error reading Excel: {e}")
    symbols_df = None

# ----------------------------
# 6) Process all symbols
# ----------------------------
if symbols_df is not None:
    symbol_list = symbols_df['Symbols'].dropna().tolist()
    no_symbols = []
    error_symbols = []
    successful_symbols = []

    batch_size = 51
    for i in range(0, len(symbol_list), batch_size):
        batch = symbol_list[i:i + batch_size]
        for index, stock_symbol in enumerate(batch, start=1):
            wait_for_internet()
            df_stock, has_candles = fetch_stock_data(stock_symbol)

            if not has_candles:
                no_symbols.append(stock_symbol)
                continue

            if df_stock.empty:
                error_symbols.append(stock_symbol)
                continue

            try:
                csv_file_path = os.path.join(output_folder_path, f"{stock_symbol}_data.csv")
                df_stock.to_csv(csv_file_path, index=False)
                print(f"✅ Saved ({i + index}/{len(symbol_list)}): {stock_symbol}")
                successful_symbols.append(stock_symbol)
            except Exception as e:
                print(f"❌ Save error for {stock_symbol}: {e}")
                error_symbols.append(stock_symbol)

        print("⏸ Waiting 20 seconds before next batch...")
        time.sleep(20)

    # Save lists to Excel
    if no_symbols:
        pd.DataFrame(no_symbols, columns=['Symbols']).to_excel(no_symbols_file_path, index=False)
        print(f"📄 No data saved: {no_symbols_file_path}")

    if error_symbols:
        pd.DataFrame(error_symbols, columns=['Symbols']).to_excel(error_log_file_path, index=False)
        print(f"📄 Errors saved: {error_log_file_path}")

    print(f"\n✅ All CSVs saved to: {output_folder_path}")
    print(f"Total: {len(symbol_list)}, Success: {len(successful_symbols)}, No Data: {len(no_symbols)}, Errors: {len(error_symbols)}")

else:
    print("❌ No symbol data found in Excel.")


# %%
import os
import sqlite3
import csv
import boto3
import io
from botocore.client import Config

# --- Cloudflare R2 credentials from GitHub Secrets ---
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY]):
    raise RuntimeError("❌ Missing R2 secrets. Please set them in GitHub Secrets.")

# --- Cloudflare R2 bucket and database key ---
R2_BUCKET = "lihan"
R2_DB_KEY = "Desktop/Database/Sorting_2022_2023.db"

# --- Local CSV source folder ---
SOURCE_FOLDER = r"C:\Users\bomma\Desktop\Daily Reports\Price\Day wise"

# --- Connect to Cloudflare R2 ---
r2 = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

# --- Step 1: Download database into memory ---
print("⬇️ Downloading database from Cloudflare R2 (in memory)...")
db_buffer = io.BytesIO()
r2.download_fileobj(R2_BUCKET, R2_DB_KEY, db_buffer)
db_buffer.seek(0)
print("✅ Database loaded into memory.")

# --- Step 2: Connect SQLite directly from memory ---
conn = sqlite3.connect(":memory:")
with sqlite3.connect("file:memdb1?mode=memory&cache=shared", uri=True) as conn:
    # Load R2 database bytes into memory
    temp_path = ":memory:"
    file_conn = sqlite3.connect(temp_path)
    with open(":memory:", "wb") as _:
        pass  # no file writing
    # Write DB bytes from buffer into memory DB
    db_bytes = db_buffer.getvalue()
    temp_file = io.BytesIO(db_bytes)
    with open(":memory:", "wb") as _:
        pass
    conn = sqlite3.connect(":memory:")
    # Load DB contents into memory connection
    with open("temp.db", "wb") as f:
        f.write(db_bytes)
    conn = sqlite3.connect("temp.db")
    cursor = conn.cursor()

# --- Step 3: Insert all CSV data ---
def sanitize_name(name: str) -> str:
    """Clean up table names to be SQLite-safe."""
    import re
    return re.sub(r"[^A-Za-z0-9_]", "_", name)

for file_name in os.listdir(SOURCE_FOLDER):
    if not file_name.lower().endswith(".csv"):
        continue

    table_name = sanitize_name(os.path.splitext(file_name)[0])
    csv_path = os.path.join(SOURCE_FOLDER, file_name)

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)

            # Create table if missing
            cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                cols = ", ".join(f'"{h}" TEXT' for h in headers)
                cursor.execute(f'CREATE TABLE "{table_name}" ({cols});')
                print(f"🆕 Created table: {table_name}")

            placeholders = ", ".join(["?"] * len(headers))
            cursor.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', reader)
            conn.commit()
            print(f"✅ Inserted data from {file_name} → {table_name}")

    except Exception as e:
        print(f"❌ Error inserting {file_name}: {e}")
        conn.rollback()

# --- Step 4: Save DB back to Cloudflare R2 ---
conn.backup(sqlite3.connect("upload_temp.db"))
conn.close()

print("⬆️ Uploading updated database back to Cloudflare R2...")
with open("upload_temp.db", "rb") as f:
    r2.upload_fileobj(f, R2_BUCKET, R2_DB_KEY)
print("✅ Upload complete!")

# --- Step 5: Clean up ---
os.remove("upload_temp.db")
print("🧹 Clean temporary file deleted.")



