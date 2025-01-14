"""
FBRef Data Collection Module

This module provides functionality for collecting individual player match data from FBRef.com.
It handles rate limiting, data processing, and combines individual player data into a single dataset.
"""

from typing import List
from requests.exceptions import HTTPError
import requests
import time
import pandas as pd
from tqdm import tqdm
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FBRefRateLimitError(Exception):
    """Custom exception for FBRef rate limit handling."""
    pass

class PlayerDataError(Exception):
    """Custom exception for player data processing errors."""
    pass

def get_individual_player_data(
    fbref_id: str, 
    fbref_name: str,
    season: str
) -> pd.DataFrame:
    """
    Fetch and process individual player match data from FBRef.

    Args:
        fbref_id (str): Player's unique identifier on FBRef
        fbref_name (str): Player's name as it appears on FBRef
        season (str): Season to collect data for.

    Returns:
        pd.DataFrame: DataFrame containing player's Premier League match data

    Raises:
        FBRefRateLimitError: If rate limit is exceeded
        PlayerDataError: If there's an error processing player data
        HTTPError: For other HTTP-related errors
    """
    
    # Construct URL for player's match logs
    fbref_name_link = fbref_name.replace(" ", "-")
    url = (f"https://fbref.com/en/players/{fbref_id}/matchlogs/{season}/"
           f"{fbref_name_link}-Match-Logs")
    
    try:
        # Make request to FBRef
        response = requests.get(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}  # Add user agent to avoid blocking
        )
        response.raise_for_status()

        # Parse HTML tables from response
        all_performances_df = pd.read_html(response.content, attrs={"id": "matchlogs_all"})[0]

        # Clean column names
        all_performances_df.columns = [
            ' '.join(col).strip() if not col[0].startswith("Unnamed") 
            else col[1] for col in all_performances_df.columns.values
        ]
        
        # Filter for Premier League matches only
        prem_performances_df = all_performances_df[all_performances_df["Comp"] == "Premier League"].copy()
        
        # Add player name column
        prem_performances_df.loc[:, "name"] = fbref_name
        
        return prem_performances_df
        

    except HTTPError as e:
        if response.status_code == 429:  # Rate limit exceeded
            wait_time = int(response.headers.get("Retry-After", 10))
            raise FBRefRateLimitError(f"Rate limit exceeded. Retry after {wait_time} seconds")
        
        else:
            raise HTTPError(f"HTTP error occurred: {e}")
    
    except Exception as e:
        raise PlayerDataError(f"Error processing data for {fbref_name}: {str(e)}")

def collect_players_data(
    df: pd.DataFrame,
    fbref_season: str,
    call_rate: int = 5,
    max_retries: int = 3,
) -> List[pd.DataFrame]:
    """
    Collect match data for multiple players with rate limiting.

    Args:
        df (pd.DataFrame): DataFrame containing player IDs and names
        call_rate (int, optional): Number of API calls per minute. Defaults to 5
        max_retries (int, optional): Maximum number of retries for failed requests. Defaults to 3

    Returns:
        List[pd.DataFrame]: List of DataFrames containing player match data

    Raises:
        ValueError: If call_rate is too high
        PlayerDataError: If there's an error collecting player data
    """
    if call_rate > 10:
        raise ValueError("Call rate too high. Must be below 10 calls per minute.")
    
    # Setting inital variables
    wait_time = 60 / call_rate
    df_list: List[pd.DataFrame] = []
    
    # Looping through players
    for idx, row in tqdm(df.iterrows(), total=len(df), desc= f"Collecting Players ({fbref_season})"):
        retries = 0
        
        # Setting loop to retry
        if isinstance(row["id_fbref"], str):
            while retries < max_retries:
                try:
                    # Get player dataframe
                    player_df = get_individual_player_data(
                        fbref_id=row["id_fbref"],
                        fbref_name=row["name_fbref"],
                        season= fbref_season
                    )
                    df_list.append(player_df)
                    time.sleep(wait_time)
                    break
                    
                except FBRefRateLimitError as e:
                    wait_time = int(str(e).split()[-2])
                    logging.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    retries += 1
                    
                except Exception as e:
                    logging.error(f"Error processing {row['name_fbref']}: {e}")
                    retries += 1
                    
            if retries == max_retries:
                logging.error(f"Failed to collect data for {row['name_fbref']} after {max_retries} attempts")
    
    return df_list

def main():
    """
    Main function to orchestrate the data collection process.
    """
    try:
        # Setup paths
        input_path = Path("./data/results/2024-25/player_compiled_ids.csv")
        output_path = Path("./data/results/2024-25/fbref_merged_gw_data.csv")
        
        # Ensure directories exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Read player data
        players = pd.read_csv(input_path)
        logging.info(f"Found {len(players)} players to process")
        
        # Collect player data
        df_list = collect_players_data(
            players,
            fbref_season= "2024-2025",
            call_rate=5,
            max_retries=3
        )
        
        # Validate and save results
        # if len(df_list) == len(players):
        #     merged_df = pd.concat(df_list, axis=0, ignore_index=True, join="outer")
        #     merged_df.to_csv(output_path, index=False)
        #     logging.info(f"Successfully saved data for {len(players)} players")
        #     return 0
        
        # else:
        #     raise PlayerDataError(
        #         f"Data collection incomplete. Expected {len(players)} players, "
        #         f"got {len(df_list)}"
        #     )
        
        # merged_df = pd.concat(df_list, axis=0, ignore_index=True, join="outer")
        # merged_df.to_csv(output_path, index=False)
        # logging.info(f"Successfully saved data for {len(players)} players")
        return 0
            
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
