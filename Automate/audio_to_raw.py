'''python spreadsheet_script.py 
--credentials path/to/credentials.json 
--token path/to/token.json 
--spreadsheet_id YOUR_SPREADSHEET_ID 
--audio_sheet_name CustomAudio 
--raw_sheet_name CustomRaw
'''

'''python audio_to_raw.py 
--credentials client_secret_70065301055-fjcq0nugl0id8k0oab6qjse8e3o24in6.apps.googleusercontent.com.json 
--token token.json --spreadsheet_id 110Sm5kozkB_yO41uYqUJs21Nuec15OWR-TemOQqhuUc 
--audio_sheet_name Audio 
--raw_sheet_name RawAuto
'''

import os
import argparse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_sheets_service(credentials_file, token_file):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(token_file, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)
        return service
    except HttpError as err:
        print(err)
        return None

def sheet_exists(service, spreadsheet_id, sheet_name):
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', [])
    for sheet in sheets:
        if sheet['properties']['title'] == sheet_name:
            return True
    return False

def create_raw_sheet(service, spreadsheet_id, sheet_name):
    requests = [
        {
            'addSheet': {
                'properties': {
                    'title': sheet_name
                }
            }
        }
    ]
    body = {
        'requests': requests
    }
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
    return response

def col_num_to_letter(n):
    """Convert a column number to a letter (e.g., 1 -> A, 27 -> AA)."""
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def get_column_count(service, spreadsheet_id, sheet_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{sheet_name}!1:1'  # Get the first row to determine the number of columns
    ).execute()
    values = result.get('values', [])
    if values:
        return len(values[0])
    return 0

def copy_columns(service, spreadsheet_id, audio_sheet_name, raw_sheet_name):
    num_columns = get_column_count(service, spreadsheet_id, audio_sheet_name)
    end_column_letter = col_num_to_letter(num_columns)

    # Get the data from the Audio sheet
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{audio_sheet_name}!A1:{end_column_letter}',  # Adjust the range as needed
        valueRenderOption='FORMULA'
    ).execute()
    audio_values = result.get('values', [])

    if not audio_values:
        print("No data found in the Audio sheet.")
        return

    # Prepare data for the Raw sheet
    raw_values = []
    for idx, row in enumerate(audio_values):
        new_row = row[:4]  # Copy columns A, B, C, D

        for col in range(4, num_columns):
            if idx == 1:
                # This is the header row, copy value as is, preserving hyperlinks
                new_row.append(row[col] if col < len(row) else "")
            elif idx == 0:
                # Skip formula for row 2
                new_row.append('')
            else:
                if col < len(row) and row[col].strip() != "":
                    col_letter = col_num_to_letter(col + 1)  # Convert column index to letter
                    new_row.append(f"=SUBSTITUTE({audio_sheet_name}!{col_letter}{idx+1}, \"RE-\", \"\", 1)*1")
                else:
                    new_row.append(0)  # If cell is blank, append 0

        raw_values.append(new_row)

    # Update the Raw sheet with the new data
    body = {
        'values': raw_values
    }
    response = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f'{raw_sheet_name}!A1',
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()
    return response

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--credentials', type=str, required=True,
                        help='Path to the credentials JSON file.')
    parser.add_argument('--token', type=str, default='token.json',
                        help='Path to the token JSON file.')
    parser.add_argument('--spreadsheet_id', type=str, required=True,
                        help='ID of the spreadsheet.')
    parser.add_argument('--audio_sheet_name', type=str, default='Audio',
                        help='Name of the Audio sheet.')
    parser.add_argument('--raw_sheet_name', type=str, default='RawAuto',
                        help='Name of the Raw sheet.')

    args = parser.parse_args()

    service = get_sheets_service(args.credentials, args.token)
    if service:
        try:
            if not sheet_exists(service, args.spreadsheet_id, args.raw_sheet_name):
                # Create Raw sheet if it does not exist
                create_raw_sheet(service, args.spreadsheet_id, args.raw_sheet_name)
                print("Sheet created.")
            
            # Copy columns and apply formula
            copy_columns(service, args.spreadsheet_id, args.audio_sheet_name, args.raw_sheet_name)
            
            print("Columns copied with formula applied.")
        except HttpError as err:
            print(err)

if __name__ == '__main__':
    main()
