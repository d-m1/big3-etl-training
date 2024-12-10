import pandas as pd
import os

def extract_data():
    output_dir = "/opt/airflow/dags/cleaned_data"
    os.makedirs(output_dir, exist_ok=True)

    for year in range(1998, 2024):
        url = f"https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_{year}.csv"
        try:
            df = pd.read_csv(url)
            df.to_parquet(f"{output_dir}/matches_{year}.parquet")
        except Exception as e:
            print(f"Error processing data for {year}: {e}")

if __name__ == "__main__":
    extract_data()