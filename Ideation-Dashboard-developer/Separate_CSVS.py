import pandas as pd
import os

# Load your main CSV file
main_file_path = "Main.csv"
df = pd.read_csv(main_file_path)

# Ensure there's an article_id column
if 'article_id' not in df.columns:
    raise ValueError("Column 'article_id' is required in the main CSV.")

# Directory to save separate CSVs
output_dir = "separated_csvs"
os.makedirs(output_dir, exist_ok=True)

# Define which columns to normalize
multi_value_columns = [
    "keywords", "sub_sectors", "products", "events", "trends", 
     "organizations", "people", "investors", "funding_rounds"
]
# Loop through and create separate CSVs
for col in multi_value_columns:
    if col in df.columns:
        records = []
        for _, row in df.iterrows():
            article_id = row["article_id"]
            values = row[col]
            if pd.notna(values):  # Skip NaNs
                split_values = [v.strip() for v in str(values).split(",") if v.strip()]
                for value in split_values:
                    records.append({"article_id": article_id, col: value})
       
        output_df = pd.DataFrame(records)
        output_path = os.path.join(output_dir, f"{col}.csv")
        output_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"Saved: {output_path}")
    else:
        print(f"Column not found: {col}")

print("\n✅ All separate CSVs created in the 'separated_csvs' folder.")
