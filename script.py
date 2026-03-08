# 🔐 Final Robust Google Sheet Automation Script (A:L Columns Only)
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import time
import random

# ------------------------
# 1. Google Credentials & Initialization
# ------------------------
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# ------------------------
# 2. Configuration
# ------------------------
folder_id = '1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo'
target_spreadsheet_id = '1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I'

excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}
listofFrames = []

# ------------------------
# 3. Helper Functions (Retry + Throttling)
# ------------------------
def safe_gspread_call(func, retries=5, wait=10):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if "429" in str(e):
                sleep_time = wait * (2 ** attempt) + random.uniform(1, 5)
                print(f"⚠️ Quota hit! Sleeping for {round(sleep_time)}s... (Attempt {attempt+1}/{retries})")
                time.sleep(sleep_time)
            else:
                print(f"❌ Error: {e}")
                return None
    return None

# ------------------------
# 4. Main Data Extraction Loop
# ------------------------
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false"
response = drive_service.files().list(q=query, pageSize=1000).execute()
files = response.get('files', [])

print(f"🚀 Found {len(files)} files. Starting extraction (Columns A:L)...")

for idx, file in enumerate(files):
    print(f"📁 [{idx+1}/{len(files)}] Processing: {file['name']}")
    
    spreadsheet = safe_gspread_call(lambda: gc.open_by_key(file['id']))
    if not spreadsheet: continue

    time.sleep(1.2) # API Pacing
    worksheets = safe_gspread_call(lambda: spreadsheet.worksheets())
    if not worksheets: continue

    for sheet in worksheets:
        if sheet.title in excluded_sheets:
            continue

        data = safe_gspread_call(lambda: sheet.get_all_values())
        if not data or len(data) < 5:
            continue

        try:
            # --- A:L Column Filtering Logic ---
            # DataFrame banakar sirf pehle 12 columns select karna
            temp_df = pd.DataFrame(data)
            temp_df = temp_df.iloc[:, :12] # Keep only first 12 columns (A to L)

            # Header row 4 (index 3) se lena aur 12 columns tak limit karna
            header = temp_df.iloc[3].to_list() 
            
            # Row 5 onwards ka data
            rows = temp_df.iloc[4:] 
            
            # Final Clean DataFrame
            df = pd.DataFrame(rows.values, columns=header)
            df = df.loc[:, ~df.columns.duplicated()] # Remove duplicates if any

            if 'Status' in df.columns:
                # Sirf meaningful rows lena
                df = df[df['Status'].notna() & (df['Status'] != '')]
                listofFrames.append(df)
                
        except Exception as e:
            print(f"   ⚠️ Skipping sheet '{sheet.title}': {e}")
    
    # Delay to respect 60 requests/min limit
    time.sleep(2)

# ------------------------
# 5. Combine and Update Master Sheets
# ------------------------
def update_master_sheet(sheet_name, dataframe):
    if dataframe.empty:
        print(f"ℹ️ No data for {sheet_name}, skipping update.")
        return
    
    print(f"📤 Updating Master Sheet: {sheet_name}...")
    ws = safe_gspread_call(lambda: gc.open_by_key(target_spreadsheet_id).worksheet(sheet_name))
    if not ws: return

    # Format for Google Sheets (List of Lists)
    final_data = [dataframe.columns.to_list()] + dataframe.astype(str).values.tolist()
    
    safe_gspread_call(lambda: ws.clear())
    time.sleep(2) 
    safe_gspread_call(lambda: ws.update(range_name='A1', values=final_data, value_input_option="USER_ENTERED"))

if listofFrames:
    combined_master = pd.concat(listofFrames, ignore_index=True)
    
    update_master_sheet("Total_IDs", combined_master)

    complete_df = combined_master[combined_master["Status"] == "Complete"]
    update_master_sheet("Complete_IDs", complete_df)

    lpe_df = combined_master[combined_master["Status"] == "LPE"]
    update_master_sheet("LPE_IDs", lpe_df)

    print("✅ All done! 100+ sheets merged (Columns A:L only).")
else:
    print("⚠️ No valid data found in any sheet.")
