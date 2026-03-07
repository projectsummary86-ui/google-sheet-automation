import gspread
import pandas as pd
import time
from google.oauth2.service_account import Credentials

FOLDER_ID = "PASTE_FOLDER_ID"
FINAL_SHEET_ID = "PASTE_FINAL_SHEET_ID"

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)

gc = gspread.authorize(creds)

drive_service = gc.auth

spreadsheet = gc.open_by_key(FINAL_SHEET_ID)
output_sheet = spreadsheet.sheet1

all_data = []

files = gc.list_spreadsheet_files()

for file in files:

    try:
        sh = gc.open_by_key(file['id'])

        for ws in sh.worksheets():

            data = ws.get_all_values()

            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])
                all_data.append(df)

            time.sleep(2)

    except Exception as e:
        print("Error:", e)

if all_data:

    final_df = pd.concat(all_data)

    output_sheet.clear()

    output_sheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())

print("Done")