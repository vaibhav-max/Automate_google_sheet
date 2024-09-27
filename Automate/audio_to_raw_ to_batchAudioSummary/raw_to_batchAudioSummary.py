'''python google_sheets_automation.py 
--spreadsheet-id YOUR_SPREADSHEET_ID 
--credentials-file YOUR_CREDENTIALS_FILE 
--token-file YOUR_TOKEN_FILE 
--raw-sheet-name YOUR_RAW_SHEET_NAME 
--target-sheet-name YOUR_TARGET_SHEET_NAME
'''

'''python raw_to_batchAudioSummary.py 
--spreadsheet-id 110Sm5kozkB_yO41uYqUJs21Nuec15OWR-TemOQqhuUc 
--credentials-file client_secret_70065301055-fjcq0nugl0id8k0oab6qjse8e3o24in6.apps.googleusercontent.com.json 
--token-file token.json 
--raw-sheet-name RawAuto 
--target-sheet-name BatchAudioSummaryAuto
'''

import os
import time
import argparse
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd


# Parameters
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

STATUS_DATA = [
    'Raw Delivered',
    'Delivered greater than acceptance threshold',
    'Raw Redelivery',
    'Redelivered greater than acceptance threshold',
    'Accepted post Initial Check (file level)',
    'Accepted post Initial check (chunk level)',
    'Accepted post automated single audio check (chunk level)',
    'Delivered for manual QC',
    'Accepted post final single Audio Manual QC (chunk level)'
]

def parse_arguments():
    parser = argparse.ArgumentParser(description="Google Sheets automation script.")
    parser.add_argument('--spreadsheet-id', type=str, required=True, help='The ID of the Google Spreadsheet.')
    parser.add_argument('--credentials-file', type=str, required=True, help='Path to the credentials JSON file.')
    parser.add_argument('--token-file', type=str, required=True, help='Path to the token JSON file.')
    parser.add_argument('--raw-sheet-name', type=str, required=True, help='Name of the raw sheet in the spreadsheet.')
    parser.add_argument('--target-sheet-name', type=str, required=True, help='Name of the target sheet in the spreadsheet.')
    parser.add_argument('--rate-limit-delay', type=float, default=0.5, help='Rate limit delay between API calls in seconds.')
    return parser.parse_args()

# Function to authenticate and create a Google Sheets API service instance
def get_sheets_service(credentials_file, token_file, scopes):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)
        return service
    except HttpError as err:
        print(err)
        return None

def col_num_to_letter(n):
    """Convert a column number to a letter (e.g., 1 -> A, 27 -> AA)."""
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# Function to get the number of rows and columns in a sheet
def get_sheet_dimensions(service, spreadsheet_id, sheet_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A2:2"
    ).execute()
    values = result.get("values", [])
    num_cols = len(values[0]) if values else 0

    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A:A"
    ).execute()
    values = result.get("values", [])
    num_rows = len(values) if values else 0

    return num_rows, num_cols

def create_or_update_sheet(service, spreadsheet_id, raw_sheet_name, target_sheet_name, rate_limit_delay):
    df = pd.DataFrame({'Status': STATUS_DATA})
    values = [df.columns.tolist()] + df.values.tolist()

    raw_num_rows, raw_num_cols = get_sheet_dimensions(service, spreadsheet_id, raw_sheet_name)

    # Check if the sheet already exists
    sheet_exists = False
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == target_sheet_name:
            sheet_id = sheet['properties']['sheetId']
            sheet_exists = True
            break

    if sheet_exists:
        # Delete the existing sheet
        delete_sheet_request = {
            "requests": [
                {
                    "deleteSheet": {
                        "sheetId": sheet_id
                    }
                }
            ]
        }
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=delete_sheet_request).execute()

    # Create a new sheet
    add_sheet_request = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": target_sheet_name,
                        "gridProperties": {
                            "rowCount": raw_num_rows,
                            "columnCount": raw_num_cols
                        }
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=add_sheet_request).execute()

    body = {
        "values": values
    }
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{target_sheet_name}!A1",
        valueInputOption="RAW",
        body=body
    ).execute()
    time.sleep(rate_limit_delay)

    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{raw_sheet_name}!2:2"
    ).execute()
    raw_headers = result.get("values", [])
    num_columns_raw = len(raw_headers[0]) if raw_headers else 0

    # Read raw data headers and first 1000 rows of each relevant column in one go
    raw_data_range = f"{raw_sheet_name}!E2:{col_num_to_letter(4 + num_columns_raw - 5)}{raw_num_rows}"
    raw_data_result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=raw_data_range
    ).execute()
    raw_data = raw_data_result.get("values", [])

    updates = []
    for col_offset in range(num_columns_raw - 4):
        formula_column = col_num_to_letter(5 + col_offset)
        col_to_update = col_num_to_letter(3 + col_offset)

        # Extract header and data for the current column
        raw_header = raw_data[0][col_offset] if raw_data and len(raw_data) > 0 and len(raw_data[0]) > col_offset else ""
        updates.append({
            "range": f"{target_sheet_name}!{col_to_update}1",
            "values": [[raw_header]]
        })

        for i in range(2, 11):
            if i == 9:
                formula = f'={col_to_update}7-{col_to_update}8'
            elif i == 10:
                formula = f'=SUMPRODUCT((MOD(ROW({raw_sheet_name}!{formula_column}{i}:{formula_column}{raw_num_rows})-ROW({raw_sheet_name}!{formula_column}{i}),8)=0)*{raw_sheet_name}!{formula_column}{i}:{formula_column}{raw_num_rows})/60'
            else:
                formula = f'=SUMPRODUCT((MOD(ROW({raw_sheet_name}!{formula_column}{i+1}:{formula_column}{raw_num_rows})-ROW({raw_sheet_name}!{formula_column}{i+1}),8)=0)*{raw_sheet_name}!{formula_column}{i+1}:{formula_column}{raw_num_rows})/60'
            
            updates.append({
                "range": f"{target_sheet_name}!{col_to_update}{i}",
                "values": [[formula]],
                "majorDimension": "ROWS"
            })

    updates.append({
        "range": f"{target_sheet_name}!B1",
        "values": [["# of Hours"]],
        "majorDimension": "ROWS"
    })

    for i in range(2, 11):
        formula = f'=SUM({target_sheet_name}!C{i}:{col_num_to_letter(ord("E") + col_offset)}{i})'
        updates.append({
            "range": f"{target_sheet_name}!B{i}",
            "values": [[formula]],
            "majorDimension": "ROWS"
        })

    data_body = {
        "valueInputOption": "USER_ENTERED",
        "data": updates
    }
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=data_body
    ).execute()
    time.sleep(rate_limit_delay)

def additional_operations(service, spreadsheet_id):
    BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_C_START_ROW = 14
    BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_D_START_ROW = 14
    BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_E_START_ROW = 14

    # Add headers to the 12th row of BatchAudioSummaryAuto sheet
    headers = [
        ["Accepted post Initial check (chunk level)", 
         "Accepted post automated single audio check (chunk level)", 
         "Accepted post final single Audio Manual QC (chunk level)"]
    ]
    
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="BatchAudioSummaryAuto!C12:E12",
        valueInputOption='RAW',
        body={'values': headers}
    ).execute()

    # Get the data from the rawAuto sheet
    raw_auto_data = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="rawAuto!A3:B"
    ).execute().get('values', [])

    # Extract unique districts and their states
    district_state_map = {}
    for row in raw_auto_data:
        if len(row) < 2:
            continue
        state, district = row
        if district not in district_state_map:
            district_state_map[district] = state

    # Prepare the data to be written to the BatchAudioSummaryAuto sheet
    data_to_write = []
    for district, state in district_state_map.items():
        data_to_write.append([state, district])

    # Write the data to the BatchAudioSummaryAuto sheet
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="BatchAudioSummaryAuto!A14:B",
        valueInputOption='RAW',
        body={'values': data_to_write}
    ).execute()

    # Prepare and write formulas to the BatchAudioSummaryAuto sheet for column C
    formulas_c_to_write = []
    row_index_c = 8
    for i in range(len(raw_auto_data)):
        formula = f"=ROUND(rawAuto!D{row_index_c}/60,2)"
        formulas_c_to_write.append([formula])
        row_index_c += 8

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="BatchAudioSummaryAuto!C14:C",
        valueInputOption='USER_ENTERED',
        body={'values': formulas_c_to_write}
    ).execute()

    # Prepare and write formulas to the BatchAudioSummaryAuto sheet for column D
    formulas_d_to_write = []
    row_index_d = 9
    for i in range(len(raw_auto_data)):
        formula = f"=ROUND(rawAuto!D{row_index_d}/60,2)"
        formulas_d_to_write.append([formula])
        row_index_d += 8

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="BatchAudioSummaryAuto!D14:D",
        valueInputOption='USER_ENTERED',
        body={'values': formulas_d_to_write}
    ).execute()

    # Prepare and write formulas to the BatchAudioSummaryAuto sheet for column E
    formulas_e_to_write = []
    row_index_e = 10
    for i in range(len(raw_auto_data)):
        formula = f"=ROUND(rawAuto!D{row_index_e}/60,2)"
        formulas_e_to_write.append([formula])
        row_index_e += 8

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="BatchAudioSummaryAuto!E14:E",
        valueInputOption='USER_ENTERED',
        body={'values': formulas_e_to_write}
    ).execute()

    # Prepare and write IF formulas to column G
    if_formulas_to_write = []
    for i in range(len(raw_auto_data)):
        formula = f"=IF(C{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_C_START_ROW + i} > 100, \"Exceeded\", C{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_C_START_ROW + i})"
        if_formulas_to_write.append([formula])

    if_range = f"BatchAudioSummaryAuto!G{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_C_START_ROW}:G{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_C_START_ROW + len(raw_auto_data) - 1}"

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=if_range,
        valueInputOption='USER_ENTERED',
        body={'values': if_formulas_to_write}
    ).execute()

    # Prepare and write IF formulas to column H based on column D values
    if_formulas_h_to_write = []
    for i in range(len(raw_auto_data)):
        formula = f"=IF(D{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_D_START_ROW + i} > 100, \"Exceeded\", D{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_D_START_ROW + i})"
        if_formulas_h_to_write.append([formula])

    if_range_h = f"BatchAudioSummaryAuto!H{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_D_START_ROW}:H{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_D_START_ROW + len(raw_auto_data) - 1}"

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=if_range_h,
        valueInputOption='USER_ENTERED',
        body={'values': if_formulas_h_to_write}
    ).execute()
    
    # Prepare and write IF formulas to column I based on column E values
    if_formulas_i_to_write = []
    for i in range(len(raw_auto_data)):
        formula = f"=IF(E{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_E_START_ROW + i} > 100, \"Exceeded\", E{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_E_START_ROW + i})"
        if_formulas_i_to_write.append([formula])

    if_range_i = f"BatchAudioSummaryAuto!I{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_E_START_ROW}:I{BATCH_AUDIO_SUMMARY_AUTO_FORMULAS_E_START_ROW + len(raw_auto_data) - 1}"
    
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=if_range_i,
        valueInputOption='USER_ENTERED',
        body={'values': if_formulas_i_to_write}
    ).execute()
    
    # Add sum formulas to row 13
    sum_formulas = [
        ["=SUM(C14:C)", "=SUM(D14:D)", "=SUM(E14:E)"]
    ]
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="BatchAudioSummaryAuto!C13:E13",
        valueInputOption='USER_ENTERED',
        body={'values': sum_formulas}
    ).execute()

def main():
    args = parse_arguments()
    service = get_sheets_service(args.credentials_file, args.token_file, SCOPES)
    if service:
        create_or_update_sheet(service, args.spreadsheet_id, args.raw_sheet_name, args.target_sheet_name, args.rate_limit_delay)
        additional_operations(service, args.spreadsheet_id)

if __name__ == "__main__":
    main()
