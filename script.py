import os
import time
import gspread

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# ---------- AUTH ----------
with open("credentials.json","w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS"])

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


# ---------- GET FILES ----------
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"

response = drive_service.files().list(
q=query,
fields="files(id,name)"
).execute()

files = response.get("files",[])

print("Total spreadsheets:",len(files))


# ---------- STORE DATA ----------
all_rows=[]
complete_rows=[]
lpe_rows=[]


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

                if not data or len(data)<4:
                    continue

                rows=data[3:]

                for r in rows:

                    if len(r)<12:
                        continue

                    status=str(r[6]).strip()

                    if status=="":
                        continue

                    row=r[:12]

                    all_rows.append(row)

                    if status.lower()=="complete":
                        complete_rows.append(row)

                    if status.lower()=="lpe":
                        lpe_rows.append(row)

            except Exception as e:
                print("Sheet skipped:",ws.title)

            # delay to avoid API quota
            time.sleep(2)

    except Exception as e:
        print("Spreadsheet skipped:",file["name"])



# ---------- WRITE RESULT ----------
sheet = gc.open_by_key(final_sheet_id)

ws_total=sheet.worksheet("Total_IDs")
ws_complete=sheet.worksheet("Complete_IDs")
ws_lpe=sheet.worksheet("LPE_IDs")


print("Clearing old data...")

ws_total.batch_clear(["A2:Z"])
ws_complete.batch_clear(["A2:Z"])
ws_lpe.batch_clear(["A2:Z"])


print("Writing new data...")

if all_rows:
    ws_total.update("A2",all_rows)

if complete_rows:
    ws_complete.update("A2",complete_rows)

if lpe_rows:
    ws_lpe.update("A2",lpe_rows)


print("SUCCESS: DATA MERGED")
