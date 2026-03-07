import os
import time
import random
import pandas as pd
import gspread

from concurrent.futures import ThreadPoolExecutor
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from gspread.exceptions import APIError


# -------- CREATE CREDENTIAL FILE --------
with open("credentials.json","w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS"])


# -------- AUTH --------
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


# -------- CONFIG --------
folder_id = "1JpmPfqOFhXCW6H1YnsI6pp07qLMLv3Qo"
final_sheet_id = "1L9QHbdpc5DZyDzrZhpQaiu4T0tWM1naQ6MO7CJXRC0I"

excluded_sheets = {
"Quota","RnD","Proxy","OE",
"FIDs","BRANDS","Section Sheet","openEnd"
}


# -------- SAFE API CALL --------
def safe_call(func,retries=5):

    for i in range(retries):

        try:
            return func()

        except APIError:

            print("Quota hit... waiting")

            time.sleep(8 + random.random())

        except Exception as e:

            print("Error:",e)

            time.sleep(4)

    return None


# -------- GET SPREADSHEETS --------
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"

response = drive_service.files().list(
q=query,
fields="files(id,name)"
).execute()

files = response.get("files",[])

print("Total Sheets Found:",len(files))


# -------- PROCESS FILE --------
def process_file(file):

    frames=[]

    print("Reading:",file["name"])

    spreadsheet = safe_call(
        lambda: gc.open_by_key(file["id"])
    )

    if spreadsheet is None:
        return frames


    worksheets = spreadsheet.worksheets()


    for ws in worksheets:

        if ws.title in excluded_sheets:
            continue


        data = safe_call(
            lambda: ws.get_all_values()
        )


        if not data or len(data)<4:
            continue


        try:

            df = pd.DataFrame(data[3:],columns=data[0])

            # remove duplicate columns
            df = df.loc[:,~df.columns.duplicated()]

            # ensure Status column exists
            if "Status" not in df.columns:
                continue


            frames.append(df)

        except Exception as e:

            print("Sheet error:",ws.title)


        time.sleep(1)


    return frames


# -------- RUN SEQUENTIAL (Quota Safe) --------
all_data=[]

with ThreadPoolExecutor(max_workers=1) as executor:

    results = executor.map(process_file,files)

    for r in results:

        if r:
            all_data.extend(r)


# -------- SAFE CONCAT --------
frames = [f for f in all_data if not f.empty]

if not frames:

    print("No data found")
    exit()


final_df = pd.concat(frames,ignore_index=True)


final_df = final_df[
final_df["Status"].notna() &
(final_df["Status"]!="")
]


# -------- UPDATE FINAL SHEET --------
sheet = gc.open_by_key(final_sheet_id)


def update_tab(name,data):

    ws = sheet.worksheet(name)

    ws.clear()

    rows = [data.columns.tolist()] + data.astype(str).values.tolist()

    safe_call(
        lambda: ws.update("A1",rows)
    )


update_tab("Total_IDs",final_df)

update_tab(
"Complete_IDs",
final_df[final_df["Status"]=="Complete"]
)

update_tab(
"LPE_IDs",
final_df[final_df["Status"]=="LPE"]
)


print("PIPELINE COMPLETED SUCCESSFULLY")
