import os
import time
import pandas as pd
import gspread

from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from gspread.exceptions import APIError


# ---------- CREATE CREDENTIAL FILE ----------
with open("credentials.json", "w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS"])


# ---------- AUTH ----------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)

gc = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)


# ---------- CONFIG ----------
folder_id = "1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo"
final_sheet_id = "1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I"

excluded_sheets = {
    "Quota","RnD","Proxy","OE",
    "FIDs","BRANDS","Section Sheet","openEnd"
}


# ---------- GET SPREADSHEETS ----------
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"

response = drive_service.files().list(
    q=query,
    fields="files(id,name)"
).execute()

files = response.get("files", [])

print("Total Sheets Found:", len(files))


# ---------- RETRY SYSTEM ----------
def safe_read(func, retries=3):

    for i in range(retries):
        try:
            return func()

        except APIError as e:

            print("Retrying API error:", e)

            time.sleep(5)

        except Exception as e:

            print("General error:", e)

            return None

    return None


# ---------- PROCESS ONE SPREADSHEET ----------
def process_spreadsheet(file):

    print("Reading:", file["name"])

    frames = []

    try:

        spreadsheet = safe_read(lambda: gc.open_by_key(file["id"]))

        if spreadsheet is None:
            return frames

        worksheets = spreadsheet.worksheets()

        for ws in worksheets:

            if ws.title in excluded_sheets:
                continue

            data = safe_read(lambda: ws.get_all_values())

            if not data or len(data) < 4:
                continue

            df = pd.DataFrame(data[3:], columns=data[0])

            # remove duplicate columns
            df = df.loc[:, ~df.columns.duplicated()]

            if "Status" not in df.columns:
                continue

            frames.append(df)

    except Exception as e:

        print("Error:", file["name"], e)

    return frames


# ---------- PARALLEL PROCESS ----------
all_data = []

with ThreadPoolExecutor(max_workers=3) as executor:

    futures = [executor.submit(process_spreadsheet, f) for f in files]

    for future in as_completed(futures):

        result = future.result()

        if result:
            all_data.extend(result)


# ---------- MERGE DATA ----------
if not all_data:

    print("No data found")
    exit()


final_df = pd.concat(all_data, ignore_index=True)

final_df = final_df[
    final_df["Status"].notna() &
    (final_df["Status"] != "")
]


# ---------- WRITE TO FINAL SHEET ----------
sheet = gc.open_by_key(final_sheet_id)


def update_sheet(name, dataframe):

    ws = sheet.worksheet(name)

    ws.clear()

    data = [dataframe.columns.tolist()] + dataframe.astype(str).values.tolist()

    ws.update("A1", data)


# TOTAL
update_sheet("Total_IDs", final_df)

# COMPLETE
update_sheet(
    "Complete_IDs",
    final_df[final_df["Status"] == "Complete"]
)

# LPE
update_sheet(
    "LPE_IDs",
    final_df[final_df["Status"] == "LPE"]
)


print("SUCCESS: Data Updated")
