import os
import json
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import time
import sys

# ... (Credentials and Setup same rahenge) ...
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

folder_id = '1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo'
target_id = '1FpB86GuKtRou9hiebmUn2eHU1bSFvqOlK_XSuqRN0vg'
excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}

listofFrames = []

def call_api(func):
    for i in range(5):
        try:
            time.sleep(1.7)
            return func()
        except Exception as e:
            if "429" in str(e):
                time.sleep(75 + (i * 20))
            else: raise e
    return None

# ------------------------
# Main Logic with Merged Header Fix
# ------------------------
response = drive_service.files().list(q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false", pageSize=1000).execute()
files = response.get('files', [])

for idx, file in enumerate(files):
    f_name = file['name']
    print(f"📄 [{idx+1}/{len(files)}] Processing: {f_name}")
    spreadsheet = call_api(lambda: gc.open_by_key(file['id']))
    if not spreadsheet: continue
    worksheets = call_api(lambda: spreadsheet.worksheets())

    for sheet in worksheets:
        if sheet.title in excluded_sheets: continue
        data = call_api(lambda: sheet.get_all_values())
        if not data or len(data) < 4: continue

        try:
            # --- THE FIX: MERGE ROW 1 AND ROW 2 HEADERS ---
            row1 = data[0][:12] # Visual Row 1
            row2 = data[1][:12] # Visual Row 2
            
            # Dono rows ko combine karke ek single header list banate hain
            combined_header = []
            for r1, r2 in zip(row1, row2):
                # Agar row1 khali hai toh r2 lo, nahi toh r1 lo
                val = r1.strip() if r1.strip() else r2.strip()
                combined_header.append(val)

            # Check: Kya "Status" ab mila?
            if 'Status' not in combined_header:
                print(f"❌ ERROR: 'Status' missing in {f_name} -> {sheet.title}")
                print(f"Debug - Combined Header found: {combined_header}")
                sys.exit(1)

            # Data Row 4 (Index 3) se start ho raha hai
            temp_df = pd.DataFrame(data).iloc[:, :12]
            rows = temp_df.iloc[3:] 
            
            df = pd.DataFrame(rows.values, columns=combined_header)
            df = df.loc[:, ~df.columns.duplicated()]

            # Status column filtering
            df['Status'] = df['Status'].astype(str).str.strip()
            df = df[df['Status'] != '']
            
            if not df.empty:
                df['Source_File'] = f_name 
                listofFrames.append(df)
                
        except Exception as e:
            print(f"❌ Logic Error: {e}")
            sys.exit(1)

# ... (Upload function same rahega) ...
def upload_master(sheet_name, final_df):
    if final_df.empty: return
    print(f"📤 Uploading to {sheet_name}...")
    ws = call_api(lambda: gc.open_by_key(target_id).worksheet(sheet_name))
    values = [final_df.columns.to_list()] + final_df.astype(str).values.tolist()
    call_api(lambda: ws.clear())
    time.sleep(5)
    call_api(lambda: ws.update(range_name='A1', values=values, value_input_option="USER_ENTERED"))

if listofFrames:
    master_df = pd.concat(listofFrames, ignore_index=True)
    upload_master("Total_IDs", master_df)
    upload_master("Complete_IDs", master_df[master_df["Status"] == "Complete"])
    upload_master("LPE_IDs", master_df[master_df["Status"] == "LPE"])
    print("✅ PROCESS COMPLETE!")
