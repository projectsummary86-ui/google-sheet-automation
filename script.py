# 🔐 Authenticate Google services
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import time
import random

# ------------------------
# Google credentials
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
# Folder & files
# ------------------------
folder_id = '1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo'
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false"
response = drive_service.files().list(q=query, pageSize=1000).execute()
files = response.get('files', [])

excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}
listofFrames = []

# ------------------------
# Safe gspread call with retry
# ------------------------
def safe_gspread_call(func, retries=5, wait=5):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            print(f"Rate limit hit / service unavailable, retry {attempt+1}/{retries}")
            if attempt < retries - 1:
                sleep_time = wait * (2 ** attempt) + random.uniform(0, 2)
                time.sleep(sleep_time)
            else:
                print(f"❌ Failed after {retries} retries: {e}")
                return None

# ------------------------
# Loop through files and sheets
# ------------------------
for file in files:
    spreadsheet = safe_gspread_call(lambda: gc.open_by_key(file['id']))
    if not spreadsheet:
        continue

    all_sheets = safe_gspread_call(lambda: [sheet.title for sheet in spreadsheet.worksheets()])
    if not all_sheets:
        continue

    selected_sheets = [name for name in all_sheets if name not in excluded_sheets]

    for sheet_name in selected_sheets:
        worksheet = safe_gspread_call(lambda: spreadsheet.worksheet(sheet_name))
        if not worksheet:
            continue

        data = safe_gspread_call(lambda: worksheet.get("A:L"))
        if not data or len(data) < 5:
            continue

        # ------------------------
        # Fix for inconsistent row lengths
        # ------------------------
        header = data[3]
        rows = [row[:len(header)] for row in data[4:]]  # trim extra columns
        df = pd.DataFrame(rows, columns=header)
        df = df.loc[:, ~df.columns.duplicated()]

        if 'Status' not in df.columns:
            continue

        listofFrames.append(df)

# ------------------------
# Function to update sheets safely
# ------------------------
def update_sheet(sheet_name, df_list):
    ws = safe_gspread_call(lambda: gc.open_by_key('1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I').worksheet(sheet_name))
    if not ws:
        return
    safe_gspread_call(lambda: ws.clear())
    safe_gspread_call(lambda: ws.update('A1', df_list, value_input_option="USER_ENTERED"))

# ------------------------
# Combine & update all target sheets
# ------------------------
if listofFrames:
    combinedf = pd.concat(listofFrames, ignore_index=True)
    combinedff = combinedf[combinedf['Status'].notna() & (combinedf['Status'] != '')]

    combinedata = [combinedff.columns.to_list()] + combinedff.astype(str).values.tolist()
    update_sheet("Total_IDs", combinedata)

    completeIds = combinedff[combinedff["Status"] == "Complete"]
    combinedata2 = [completeIds.columns.to_list()] + completeIds.astype(str).values.tolist()
    update_sheet("Complete_IDs", combinedata2)

    LPEIds = combinedff[combinedff["Status"] == "LPE"]
    combinedata3 = [LPEIds.columns.to_list()] + LPEIds.astype(str).values.tolist()
    update_sheet("LPE_IDs", combinedata3)

    print("✅ All sheets updated successfully!")
else:
    print("⚠️ No valid data found in selected sheets.")
