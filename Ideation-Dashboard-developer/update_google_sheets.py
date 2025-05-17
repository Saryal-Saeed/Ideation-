import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os

# === STEP 1: Connect to Google Sheets ===
SERVICE_ACCOUNT_FILE = 'path/to/your/credentials.json'  # <- CHANGE THIS
SPREADSHEET_NAME = 'Your Insights Dashboard'  # <- The name of your Google Sheet

# Setup auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)

# Open the spreadsheet
spreadsheet = client.open(SPREADSHEET_NAME)

# === STEP 2: List all local CSVs and matching Sheet names ===
csv_to_sheet_mapping = {
    "Main.csv": "Main",
    "sub_sectors.csv": "Sub Sectors",
    "trends.csv": "Trends",
    "market_gaps.csv": "Market Gaps",
    "innovations.csv": "Innovations",
    "keywords.csv": "Keywords",
    "locations.csv": "Locations",
    "products.csv": "Products",
    "people.csv": "People",
    "organizations.csv": "Organizations",
    "events.csv": "Events",
    "funding_rounds.csv": "Funding Rounds",
    "investors.csv": "Investors"
}

# === STEP 3: Upload each CSV ===
for filename, sheet_name in csv_to_sheet_mapping.items():
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        continue

    print(f"⬆️ Uploading: {filename} to '{sheet_name}'...")
    df = pd.read_csv(filename)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"✅ Updated: {sheet_name}")
    except Exception as e:
        print(f"⚠️ Could not update {sheet_name}: {e}")
