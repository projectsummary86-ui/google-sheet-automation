import os
import gspread
import pandas as pd
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# GitHub secret se credentials.json create
with open("credentials.json", "w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS"])

FOLDER_ID = "1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo"
FINAL_SHEET_ID = "1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)

gc = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# Final sheet open
spreadsheet = gc.open_by_key(FINAL_SHEET_ID)
output_sheet = spreadsheet.sheet1

all_data = []

# Folder me files list karo
query = f"'{FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"

results = drive_service.files().list(
    q=query,
    fields="files(id, name)"
).execute()

files = results.get("files", [])

print("Total Sheets Found:", len(files))

for file in files:

    try:

        print("Reading:", file["name"])

        sh = gc.open_by_key(file["id"])

        for ws in sh.worksheets():

            data = ws.get_all_values()

            if len(data) > 1:

                df = pd.DataFrame(data[1:], columns=data[0])
                df["source_file"] = file["name"]
                df["sheet_name"] = ws.title

                all_data.append(df)

            time.sleep(1)

    except Exception as e:

        print("Error:", file["name"], e)

if all_data:

    final_df = pd.concat(all_data, ignore_index=True)

    print("Total rows:", len(final_df))

    output_sheet.clear()

    output_sheet.update(
        [final_df.columns.values.tolist()] + final_df.values.tolist()
    )

print("DONE")

