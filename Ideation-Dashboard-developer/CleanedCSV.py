import json
import pandas as pd

# Load JSON file
with open("extracted_insights4.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# Convert lists to comma-separated strings
for entry in data:
    for key, value in entry.items():
        if isinstance(value, list):  # Convert lists to comma-separated strings
            entry[key] = ", ".join(value)
        elif value is None:  # Replace None values
            entry[key] = "N/A" if isinstance(value, str) else 0

# Convert to DataFrame
df = pd.DataFrame(data)

# Standardize column names for Looker Studio (remove spaces/special chars)
df.columns = [col.lower().replace(" ", "_") for col in df.columns]

# Save as CSV
df.to_csv("looker_studio_data.csv", index=False, encoding="utf-8")

print("CSV file saved as looker_studio_data.csv")
