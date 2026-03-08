# 🔐 Strict Google Sheet Merger (Fails on Formatting Errors)
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import time
import sys

# ------------------------
# 1. Credentials Setup
# ------------------------
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# ------------------------
# 2. Config
# ------------------------
folder_id = '1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo'
target_spreadsheet_id = '1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I'
excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}

listofFrames = []

# ------------------------
# 3. Safe Call Logic (Only for API Quota)
# ------------------------
def safe_api_call(func, retries=5):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if "429" in str(e):
                wait = 45 + (attempt * 15)
                print(f"⏳ Quota hit, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise e # Baaki errors ke liye crash hone do
    return None

# ------------------------
# 4. Processing Files (Strict Mode)
# ------------------------
response = drive_service.files().list(
    q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false",
    pageSize=1000
).execute()
files = response.get('files', [])

print(f"🚀 Processing {len(files)} files strictly...")

for idx, file in enumerate(files):
    f_name = file['name']
    print(f"📄 [{idx+1}/{len(files)}] Checking: {f_name}")
    
    spreadsheet = safe_api_call(lambda: gc.open_by_key(file['id']))
    time.sleep(2)
    worksheets = safe_api_call(lambda: spreadsheet.worksheets())

    for sheet in worksheets:
        s_name = sheet.title
        if s_name in excluded_sheets:
            continue

        data = safe_api_call(lambda: sheet.get_all_values())
        
        # Validation 1: Minimum Rows check
        if not data or len(data) < 5:
            # Agar sheet empty hai toh error dekar stop karein
            print(f"❌ ERROR: Sheet '{s_name}' in File '{f_name}' is empty or has less than 5 rows!")
            sys.exit(1)

        # A:L Filtering
        temp_df = pd.DataFrame(data).iloc[:, :12]
        header = temp_df.iloc[3].to_list()
        rows = temp_df.iloc[4:]
        
        df = pd.DataFrame(rows.values, columns=header)
        df = df.loc[:, ~df.columns.duplicated()]

        # Validation 2: Status Column check
        if 'Status' not in df.columns:
            print(f"❌ FORMAT ERROR: 'Status' column missing in Sheet: '{s_name}', File: '{f_name}'")
            print(f"Found columns: {header}")
            sys.exit(1) # Script yahi ruk jayegi

        # Filter and Append
        df = df[df['Status'].notna() & (df['Status'] != '')]
        if not df.empty:
            listofFrames.append(df)
        
        time.sleep(1.5)
    time.sleep(4)

# ------------------------
# 5. Final Upload
# ------------------------
def final_upload(sheet_name, dataframe):
    if dataframe.empty: return
    ws = safe_api_call(lambda: gc.open_by_key(target_spreadsheet_id).worksheet(sheet_name))
    upload_data = [dataframe.columns.to_list()] + dataframe.astype(str).values.tolist()
    
    safe_api_call(lambda: ws.clear())
    time.sleep(5)
    safe_api_call(lambda: ws.update(range_name='A1', values=upload_data, value_input_option="USER_ENTERED"))
    time.sleep(5)

if listofFrames:
    master_df = pd.concat(listofFrames, ignore_index=True)
    final_upload("Total_IDs", master_df)
    final_upload("Complete_IDs", master_df[master_df["Status"] == "Complete"])
    final_upload("LPE_IDs", master_df[master_df["Status"] == "LPE"])
    print("✅ SUCCESS: All sheets merged perfectly!")
