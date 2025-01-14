import configparser
import boto3
import os
from typing import Optional, List

def load_s3_bucket():
    parser = configparser.ConfigParser()
    parser.read("/usr/local/airflow/include/data/utils/pipeline.conf")
    access_key = parser.get("aws_boto_credentials",
                    "access_key")
    secret_key = parser.get("aws_boto_credentials",
                    "secret_key")
    bucket_name = parser.get("aws_boto_credentials",
                    "bucket_name")

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    
    return s3, bucket_name

def upload_season_files(s3, bucket_name: str, season: str, update: Optional[bool] = True):
    if update:
        files = ["fbref_merged_gw_data.csv", "merged_gw.csv", "players_clean.csv",
                 "fbref_merged_gw_data.csv", "fbref_most_recent_gw.csv"]
    
    else:
        files = ["player_idlist.csv", "fbref_merged_gw_data.csv", "fbref_ids.csv",
                "player_compiled_ids.csv", "missing_fbref_ids.json", "teams.csv", "merged_gw.csv",
                "fixtures.csv", "players_clean.csv"]
    
    for file in files:
        try:
            local_file = f"/usr/local/airflow/include/data/results/{season}/" + file
            s3_key = f"data/{season}/{file}"
            s3.upload_file(
                local_file,
                bucket_name,
                s3_key)
            
            # os.remove(local_file)
            print(f"File {file} successfully uploaded.")
        except FileNotFoundError as e:
            print(e)

def upload_seasons(s3, bucket_name):
    seasons = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]
    for season in seasons:
        upload_season_files(s3, bucket_name, season, False)

def update_recent_files(season: str):
    s3, bucket_name = load_s3_bucket()
    upload_season_files(s3, bucket_name, season, True)

def download_files(files_to_download: List[str], season: str, local_dir: str= "/usr/local/airflow/include/data/results"):
    s3, bucket_name = load_s3_bucket()
    
    for file in files_to_download:
        s3_key = f"data/{season}/{file}"
        local_file_path = f"{local_dir}/{season}/{file}"
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        try:
            s3.download_file(bucket_name, s3_key, local_file_path)
            print(f"Downloaded {s3_key} to {local_file_path}")
        except s3.exceptions.NoSuchKey:
            print(f"File not found in S3: {s3_key}")
        except Exception as e:
            print(f"Error downloading {s3_key}: {e}")

def main():
    s3, bucket_name = load_s3_bucket()
    files = ["/usr/local/airflow/include/data/results/2024-25/missing_fbref_ids.json", "/usr/local/airflow/include/data/results/2024-25/player_compiled_ids.csv"]
    for file in files:
        file_name = file.split("/")[-1]
        s3_key = f"data/2024-25/{file_name}"
        s3.upload_file(
            file,
            bucket_name,
            s3_key)


if __name__ == "__main__":
    s3, bucket_name = load_s3_bucket()
    upload_season_files(s3, bucket_name, '2024-25', False)
    download_files(["merged_gw.csv", "fbref_merged_gw_data.csv"], season= '2024-25')