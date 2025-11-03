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
DOWNLOAD_FOLDER = "price_nodu/Bomma"
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
excel_file_path ='price_nodu/Bomma/Symbols 2022.xlsx'
output_folder_path ='price_nodu/Bomma/Price/Day wise'
no_symbols_file_path ='price_nodu/Bomma/Price/No download/No symbols.xlsx'
error_log_file_path ='price_nodu/Bomma/Price/error/Error log.xlsx'
log_folder_path ='price_nodu/Bomma'

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


# Make a NEW SQLite DB from CSVs, then upload to Cloudflare R2 (no download)
import os, csv, sqlite3, sys, tempfile
import boto3
from botocore.client import Config

# --- Cloudflare R2 (read from GitHub Secrets or env) ---
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY]):
    print("Missing R2 secrets."); sys.exit(1)

R2_BUCKET = "lihan"
R2_DB_KEY = "Desktop/Database/Sorting_2022_2023.db"  # will be overwritten

# --- CSV source folder (your path inside repo) ---
SOURCE_FOLDER = "price_nodu/Bomma/Price/Day wise"

# --- R2 client ---
r2 = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

def sanitize(name: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9_]", "_", name.strip())

def build_db_from_csvs(db_path: str, folder: str):
    if not os.path.isdir(folder):
        print(f"CSV folder not found: {folder}"); sys.exit(1)

    csv_files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not csv_files:
        print("No CSV files found."); sys.exit(0)

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-20000;")

    for fname in csv_files:
        table = sanitize(os.path.splitext(fname)[0])
        fpath = os.path.join(folder, fname)

        with open(fpath, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                continue

            # create table if missing (TEXT columns—simple and safe)
            cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            if not cur.fetchone():
                cols = ", ".join(f'"{h}" TEXT' for h in headers)
                cur.execute(f'CREATE TABLE "{table}" ({cols});')

            placeholders = ", ".join("?" for _ in headers)
            cols_clause  = ", ".join(f'"{h}"' for h in headers)
            sql = f'INSERT INTO "{table}" ({cols_clause}) VALUES ({placeholders})'

            batch, total, batch_size = [], 0, 5000
            for row in reader:
                if len(row) != len(headers):
                    continue
                batch.append(row)
                if len(batch) >= batch_size:
                    cur.executemany(sql, batch); total += len(batch); batch.clear()
            if batch:
                cur.executemany(sql, batch); total += len(batch)

    conn.commit()
    cur.execute("VACUUM;")
    conn.close()

def main():
    # temp DB on runner; removed after upload
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
        db_path = tmp.name
    try:
        build_db_from_csvs(db_path, SOURCE_FOLDER)
        # upload to R2 (overwrite)
        r2.upload_file(db_path, R2_BUCKET, R2_DB_KEY)
        print("GitHub Database price done ✅")  # your message box
    except Exception as e:
        print(f"Failed: {e}")
        sys.exit(1)
    finally:
        try: os.remove(db_path)
        except OSError: pass

if __name__ == "__main__":
    main()
# %%
import os
import requests

def send_telegram_message(message: str):
    """Send a Telegram message using credentials from environment variables."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("⚠️ Telegram credentials not set in environment.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("✅ Telegram message sent successfully!")
        else:
            print(f"⚠️ Telegram send failed: {response.text}")
    except Exception as e:
        print(f"❌ Error sending Telegram message: {e}")

# ----------------------------
# Your main logic code runs here...
# Example: after uploading DB to Cloudflare or finishing the main process
# ----------------------------

try:
    # Example main part:
    print("✅ All database updates completed successfully.")
    # Now send Telegram notification
    send_telegram_message("GitHub Database price done ✅")

except Exception as e:
    print(f"❌ An error occurred: {e}")
    send_telegram_message(f"❌ GitHub Database price failed: {e}")



