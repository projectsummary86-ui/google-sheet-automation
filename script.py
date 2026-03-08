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
target_id = '1FpB86GuKtRou9hiebmUn2eHU1bSFvqOlK_XSuqRN0vg'
excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}

listofFrames = []

# ------------------------
# 2. Smart API Call (Handles 429 & 503)
# ------------------------
def call_api(func):
    """Handles API Quota (429) and Server Unavailable (503) errors."""
    max_retries = 6
    for i in range(max_retries):
        try:
            # 2.0s gap taaki 60 RPM se niche rahein
            time.sleep(2.0) 
            return func()
        except Exception as e:
            err = str(e)
            # Agar rate limit (429) ya server down (503) hai
            if "429" in err or "503" in err:
                wait = 85 + (i * 30)
                print(f"⚠️ Server/Quota Issue: {err[:50]}. Waiting {wait}s... (Attempt {i+1}/{max_retries})")
                time.sleep(wait)
            else:
                # Baaki serious errors par crash hone do taaki hum dekh sakein kya hua
                print(f"❌ Permanent Error: {err}")
                raise e
    return None

# ------------------------
# 3. Data Extraction with Merged Header Fix
# ------------------------
response = drive_service.files().list(
    q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false",
    pageSize=1000
).execute()
files = response.get('files', [])

print(f"🚀 Found {len(files)} files. Starting FULL SCAN.")

for idx, file in enumerate(files):
    f_name = file['name']
    print(f"📄 [{idx+1}/{len(files)}] Processing: {f_name}")
    
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
            # --- THE FIX: MERGE ROW 1 AND ROW 2 FOR HEADERS ---
            row1 = data[0][:12] # Visual Row 1
            row2 = data[1][:12] # Visual Row 2
            
            combined_header = []
            for r1, r2 in zip(row1, row2):
                # Agar row1 merged cell hai aur khali dikh rahi hai, toh row2 se lo
                val = r1.strip() if r1.strip() else r2.strip()
                combined_header.append(val)

            # --- Validation ---
            if 'Status' not in combined_header:
                print(f"❌ ERROR: 'Status' missing in {f_name} -> {sheet.title}")
                print(f"Detected Headers: {combined_header}")
                sys.exit(1)

            # Data Row 4 (Index 3)
            temp_df = pd.DataFrame(data).iloc[:, :12]
            rows = temp_df.iloc[3:] 
            
            df = pd.DataFrame(rows.values, columns=combined_header)
            df = df.loc[:, ~df.columns.duplicated()]

            # Clean Status Column
            df['Status'] = df['Status'].astype(str).str.strip()
            df = df[df['Status'] != '']
            
            if not df.empty:
                df['Source_File'] = f_name 
                listofFrames.append(df)
                
        except Exception as e:
            print(f"❌ Logic Error in {f_name} -> {sheet.title}: {e}")
            sys.exit(1)

# ------------------------
# 4. Master Upload Function
# ------------------------
def upload_master(sheet_name, final_df):
    if final_df.empty:
        print(f"ℹ️ No data for {sheet_name}")
        return
    
    print(f"📤 Uploading {len(final_df)} rows to {sheet_name}...")
    ws = call_api(lambda: gc.open_by_key(target_id).worksheet(sheet_name))
    if not ws: return
    
    values = [final_df.columns.to_list()] + final_df.astype(str).values.tolist()
    
    call_api(lambda: ws.clear())
    time.sleep(5)
    call_api(lambda: ws.update(range_name='A1', values=values, value_input_option="USER_ENTERED"))

# --- Final Step ---
if listofFrames:
    master_df = pd.concat(listofFrames, ignore_index=True)
    upload_master("Total_IDs", master_df)
    upload_master("Complete_IDs", master_df[master_df["Status"] == "Complete"])
    upload_master("LPE_IDs", master_df[master_df["Status"] == "LPE"])
    print("✅ MISSION SUCCESS: All data merged!")
else:
    print("⚠️ No data found to merge.")
