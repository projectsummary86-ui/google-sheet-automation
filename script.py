import os
import time
import gspread

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# ---------- CREATE CREDENTIAL FILE ----------
with open("credentials.json","w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS"])


# ---------- AUTH ----------
SCOPES = [
"https://www.googleapis.com/auth/spreadsheets",
"https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
"credentials.json",
scopes=SCOPES
)

gc = gspread.authorize(creds)

drive_service = build("drive","v3",credentials=creds)


# ---------- CONFIG ----------
folder_id = "1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo"
final_sheet_id = "1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I"

excluded_sheets = {
"Quota","RnD","Proxy","OE",
"FIDs","BRANDS","Section Sheet","openEnd"
}


# ---------- GET SPREADSHEETS ----------
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"

response = drive_service.files().list(
q=query,
fields="files(id,name)"
).execute()

files = response.get("files",[])

print("Total spreadsheets:",len(files))


# ---------- STORE FINAL DATA ----------
all_rows = []


# ---------- READ FILES ----------
for file in files:

    print("Reading:",file["name"])

    try:

        spreadsheet = gc.open_by_key(file["id"])

        worksheets = spreadsheet.worksheets()

        for ws in worksheets:

            if ws.title in excluded_sheets:
                continue


            try:

                data = ws.get_all_values()

                if not data or len(data) < 4:
                    continue


                # data starts from row 4
                rows = data[3:]


                for r in rows:

                    if len(r) < 12:
                        continue


                    status = str(r[6]).strip()


                    # skip blank status
                    if status == "":
                        continue


                    # take only first 12 columns
                    all_rows.append(r[:12])


            except Exception as e:

                print("Sheet skipped:",ws.title)


            # delay to avoid API quota
            time.sleep(2)


    except Exception as e:

        print("Spreadsheet skipped:",file["name"])


# ---------- WRITE FINAL DATA ----------
sheet = gc.open_by_key(final_sheet_id)

ws = sheet.worksheet("Total_IDs")


print("Writing data...")

if all_rows:

    ws.update("A2",all_rows)

print("SUCCESS: DATA MERGED")
