import requests
import csv
import os
from google.cloud import storage

# ────────────────────────────────────────────────────────────
# 1) Call Cricbuzz API
# ────────────────────────────────────────────────────────────
url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'
headers = {
    "x-rapidapi-key": "7518d81368msh84975cc48b44394p1eaf08jsn93fed2b1978e",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}
params = {'formatType': 'odi'}

response = requests.get(url, headers=headers, params=params)

# ────────────────────────────────────────────────────────────
# 2) Convert response to CSV
# ────────────────────────────────────────────────────────────
if response.status_code == 200:
    data = response.json().get('rank', [])      # list of batsmen
    csv_filename = 'batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']

        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            # writer.writeheader()            # uncomment if header row wanted
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")

        # ─────────────────────────────────────────────────────
        # 3) Upload CSV to GCS
        # ─────────────────────────────────────────────────────
        bucket_name = 'bkt-batsmen-ranking'

        try:
            # In Cloud Composer we rely on **Application Default Credentials**,
            # so no JSON key file is needed:
            storage_client = storage.Client()

            # --- removed on‑prem keyfile block ------------------
            # SERVICE_ACCOUNT_KEY_PATH = "C:\\Users\\admin\\Downloads\\airy-digit-465111-r1-1ef5acdeaa30.json"
            # storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_PATH)
            # ----------------------------------------------------

            bucket = storage_client.bucket(bucket_name)
            blob   = bucket.blob(csv_filename)
            blob.upload_from_filename(csv_filename)
            print(f"File '{csv_filename}' uploaded to bucket '{bucket_name}'")

        except Exception as e:
            print(f"An error occurred during GCS upload: {e}")

    else:
        print("No data available from the API.")
else:
    print("Failed to fetch data:", response.status_code)
