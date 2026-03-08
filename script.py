import os
import json
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import time
import sys

# ------------------------
# 1. Credentials & Setup
# ------------------------
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

folder_id = '1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo'
# Aapki updated target ID
target_id = '1FpB86GuKtRou9hiebmUn2eHU1bSFvqOlK_XSuqRN0vg'
excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}

listofFrames = []

# ------------------------
# 2. Strict API Call (Respecting 60 RPM)
# ------------------------
def call_api(func):
    max_retries = 5
    for i in range(max_retries):
        try:
            time.sleep(1.7) # Thoda aur safe margin
            return func()
        except Exception as e:
            if "429" in str(e):
                wait = 75 + (i * 20)
                print(f"⏳ Quota limit hit. Waiting {wait}s...")
                time.sleep(wait)
            else:
                raise e
    return None

# ------------------------
# 3. Data Extraction
# ------------------------
response = drive_service.files().list(
    q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false",
    pageSize=1000
).execute()
files = response.get('files', [])

print(f"🚀 Found {len(files)} files. Starting FULL SCAN (A:L only).")

for idx, file in enumerate(files):
    f_name = file['name']
    print(f"📄 [{idx+1}/{len(files)}] Reading: {f_name}")
    
    spreadsheet = call_api(lambda: gc.open_by_key(file['id']))
    if not spreadsheet: continue
    
    worksheets = call_api(lambda: spreadsheet.worksheets())
    if not worksheets: continue

    for sheet in worksheets:
        if sheet.title in excluded_sheets:
            continue

        data = call_api(lambda: sheet.get_all_values())
        if not data or len(data) < 4:
            continue

        try:
            # --- Smart Header Search (Merged Cell Friendly) ---
            header_row_index = -1
            # Pehli 6 rows scan karenge kyunki headers upar-niche ho sakte hain
            for i, row in enumerate(data[:6]):
                # Strip spaces taaki ' Status ' jaisa data bhi match ho jaye
                clean_row = [str(c).strip() for c in row]
                if 'Status' in clean_row:
                    header_row_index = i
                    break
            
            if header_row_index == -1:
                print(f"❌ ERROR: 'Status' column dhoondne me fail! File: {f_name} -> {sheet.title}")
                sys.exit(1)

            # Columns filter A:L (0 to 11)
            temp_df = pd.DataFrame(data).iloc[:, :12]
            header = temp_df.iloc[header_row_index].to_list()
            
            # Data hamesha Row 4 (Index 3) se start ho raha hai
            rows = temp_df.iloc[3:] 
            
            # DataFrame banana
            df = pd.DataFrame(rows.values, columns=header)
            
            # Duplicate column names handle karna (e.g. do empty columns)
            df = df.loc[:, ~df.columns.duplicated()]

            # Validation check
            if 'Status' not in df.columns:
                print(f"❌ ERROR: 'Status' column missing in DataFrame structure: {f_name} -> {sheet.title}")
                sys.exit(1)

            # Clean Status column
            df['Status'] = df['Status'].astype(str).str.strip()
            df = df[df['Status'] != '']
            df = df[df['Status'].notna()]

            if not df.empty:
                df['Source_File'] = f_name 
                listofFrames.append(df)
                
        except Exception as e:
            print(f"❌ Logic Error in {f_name} -> {sheet.title}: {e}")
            sys.exit(1)

# ------------------------
# 4. Uploading to Master Sheet
# ------------------------
def upload_master(sheet_name, final_df):
    if final_df.empty:
        print(f"ℹ️ No data for {sheet_name}")
        return
    
    print(f"📤 Uploading {len(final_df)} rows to {sheet_name}...")
    ws = call_api(lambda: gc.open_by_key(target_id).worksheet(sheet_name))
    if not ws: return
    
    # Header + Data conversion
    values = [final_df.columns.to_list()] + final_df.astype(str).values.tolist()
    
    call_api(lambda: ws.clear())
    time.sleep(5)
    # Modern gspread update syntax
    call_api(lambda: ws.update(range_name='A1', values=values, value_input_option="USER_ENTERED"))

# --- Execution ---
if listofFrames:
    master_df = pd.concat(listofFrames, ignore_index=True)
    
    upload_master("Total_IDs", master_df)
    
    # Status wise filter
    upload_master("Complete_IDs", master_df[master_df["Status"] == "Complete"])
    upload_master("LPE_IDs", master_df[master_df["Status"] == "LPE"])
    
    print("✅ PROCESS COMPLETE!")
else:
    print("⚠️ No data found to merge.")
