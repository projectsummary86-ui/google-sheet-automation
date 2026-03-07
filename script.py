import os
import gspread
import pandas as pd
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Create credentials.json from GitHub secret
with open("credentials.json", "w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS"])

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)

gc = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# 📁 Folder ID
folder_id = "1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo"

# 📄 Final Sheet
final_sheet_id = "1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I"

# ❌ Sheets to skip
excluded_sheets = {"Quota", "RnD", "Proxy", "OE", "FIDs", "BRANDS", "Section Sheet", "openEnd"}

# 📂 Get spreadsheets inside folder
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"

response = drive_service.files().list(
    q=query,
    fields="files(id,name)"
).execute()

files = response.get("files", [])

print("Total spreadsheets found:", len(files))

frames = []

for file in files:

    try:

        spreadsheet = gc.open_by_key(file["id"])

        worksheets = spreadsheet.worksheets()

        for ws in worksheets:

            if ws.title in excluded_sheets:
                continue

            data = ws.get_all_values()

            if len(data) < 4:
                continue

            df = pd.DataFrame(data[3:], columns=data[0])

            df = df.loc[:, ~df.columns.duplicated()]

            if "Status" not in df.columns:
                continue

            frames.append(df)

        time.sleep(0.5)

    except Exception as e:

        print("Error reading:", file["name"], e)

# 📊 Combine all data
if frames:

    combinedf = pd.concat(frames, ignore_index=True)

    combinedff = combinedf[
        combinedf["Status"].notna() &
        (combinedf["Status"] != "")
    ]

    sheet = gc.open_by_key(final_sheet_id)

    # TOTAL
    total_data = [combinedff.columns.tolist()] + combinedff.astype(str).values.tolist()

    ws_total = sheet.worksheet("Total_IDs")
    ws_total.clear()
    ws_total.update("A1", total_data)

    # COMPLETE
    complete = combinedff[combinedff["Status"] == "Complete"]
    complete_data = [complete.columns.tolist()] + complete.astype(str).values.tolist()

    ws_complete = sheet.worksheet("Complete_IDs")
    ws_complete.clear()
    ws_complete.update("A1", complete_data)

    # LPE
    lpe = combinedff[combinedff["Status"] == "LPE"]
    lpe_data = [lpe.columns.tolist()] + lpe.astype(str).values.tolist()

    ws_lpe = sheet.worksheet("LPE_IDs")
    ws_lpe.clear()
    ws_lpe.update("A1", lpe_data)

    print("SUCCESS: Data updated")

else:

    print("No valid data found")
