# 🔐 Authenticate Google services
import os
import json
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import time

# Google credentials
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)

# Drive API
from googleapiclient.discovery import build
drive_service = build('drive', 'v3', credentials=creds)
folder_id = '1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo'
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false"
response = drive_service.files().list(q=query, pageSize=1000).execute()
files = response.get('files', [])

listofFrames = []
excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}

# Helper function for retry
def open_sheet_with_retry(gc, key, retries=3, wait=5):
    for i in range(retries):
        try:
            return gc.open_by_key(key)
        except Exception as e:
            print(f"Attempt {i+1} failed: {e}")
            if i < retries - 1:
                time.sleep(wait)
            else:
                raise

# Loop through each file
for file in files:
    spreadsheet = open_sheet_with_retry(gc, file['id'])
    all_sheets = [sheet.title for sheet in spreadsheet.worksheets()]
    selected_sheets = [name for name in all_sheets if name not in excluded_sheets]

    for sheet_name in selected_sheets:
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get("A:L")

        if len(data) < 5:
            continue

        # Create DataFrame safely
        df = pd.DataFrame(data[4:])
        header = data[3]
        df = df.iloc[:, :len(header)]
        df.columns = header[:len(df.columns)]

        df = df.loc[:, ~df.columns.duplicated()]

        if 'Status' not in df.columns:
            continue

        listofFrames.append(df)

# Combine all DataFrames
if listofFrames:
    combinedf = pd.concat(listofFrames, ignore_index=True)
    combinedff = combinedf[combinedf['Status'].notna() & (combinedf['Status'] != '')]

    combinedata = [combinedff.columns.to_list()] + combinedff.astype(str).values.tolist()

    # Update Total_IDs
    worksheetms = gc.open_by_key('1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I').worksheet("Total_IDs")
    worksheetms.clear()
    worksheetms.update('A1', combinedata, value_input_option="USER_ENTERED")

    # Filter Complete
    completeIds = combinedff[combinedff["Status"] == "Complete"]
    combinedata2 = [completeIds.columns.to_list()] + completeIds.astype(str).values.tolist()
    worksheetms2 = gc.open_by_key('1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I').worksheet("Complete_IDs")
    worksheetms2.clear()
    worksheetms2.update('A1', combinedata2, value_input_option="USER_ENTERED")

    # Filter LPE
    LPEIds = combinedff[combinedff["Status"] == "LPE"]
    combinedata3 = [LPEIds.columns.to_list()] + LPEIds.astype(str).values.tolist()
    worksheetms3 = gc.open_by_key('1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I').worksheet("LPE_IDs")
    worksheetms3.clear()
    worksheetms3.update('A1', combinedata3, value_input_option="USER_ENTERED")

else:
    print("⚠️ No valid data found in selected sheets.")
