"""

FBRef ID Collection Module

This module provides functionality for collecting player ids from FBRef.com.

"""
import os
import sys
sys.path.insert(0, os.path.abspath('.'))

import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv
import json
import logging
from typing import List, Dict, Optional
from tqdm import tqdm
from include.data.collect_data.scrapers.fbref_get_data import FBRefRateLimitError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TeamDataError(Exception):
    """Custom exception for team data processing errors."""
    pass

class PlayerDataError(Exception):
    """Custom exception for player data processing errors."""
    pass

def countdown(wait_time: int) -> None:
    """
    Displays a countdown timer for rate limiting delays.
    
    Args:
        wait_time (int): Number of seconds to wait
    """
    
    for remaining in range(wait_time, 0, -1):
        print(f"\rRetrying in {remaining} seconds...", end="")
        time.sleep(1)
    print("\rRetrying now...                  ")

def get_url_list(link: str, max_retries: Optional[int] = 3) -> List[str]:
    """
    Retrieves all player URLs from a team page with rate limit handling.
    
    Args:
        link (str): The team page URL to scrape
        max_retries (int, optional): Number of retry attempts if rate limited
    
    Returns:
        List[str]: List of player URLs
        
    Raises:
        Exception: If page retrieval fails after all retries
        
    """
    
    retries = 0
    
    while retries < max_retries:
        try:
            
            # Parsing data
            response = requests.get(
                link,
                headers= {'User-Agent': 'Mozilla/5.0'}
            )
            response.raise_for_status()
        
            soup = BeautifulSoup(response.text, 'html.parser')
            
            player_links = soup.find_all('a', href=lambda href: href and '/en/players/' in href)
            player_urls = [link["href"] for link in player_links]
            
            return player_urls
        
        except HTTPError as e:    
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 10))
                logging.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1

            
            else:
                raise HTTPError(f"HTTP error occurred: {e}")
            
        except Exception as e:
            raise TeamDataError(f"Error processing data: {str(e)}")
            

def get_ids(url_list: List[str]) -> List[Dict[str, str]]:
    """
    Extracts player IDs and names from URLs and removes duplicates.
    
    Args:
        url_list (List[str]): List of player URLs
    
    Returns:
        List[Dict[str, str]]: List of dictionaries containing player names and IDs
    """
    
    if not url_list:
        raise ValueError("Url list cannot be empty")
    
    try:
        
        ids = []
        
        for url in url_list:
            url_content_list = str(url).split("/")
            if len(url_content_list) == 5:
                player_data = {}
                
                name = url_content_list[-1].replace("-", " ")
                id = url_content_list[3]
                
                player_data["name"] = name
                player_data["id"] = id
                
                ids.append(player_data)
                
        # Remove duplicates while preserving order
        unique_ids = [dict(t) for t in {tuple(d.items()) for d in ids}]
        return unique_ids
    
    except Exception as e:
        raise PlayerDataError(f"Error processing player URLs: {str(e)}")
    

def collect_team_players(club_link_dict: Dict[str, str],
                         season: str,
                         call_rate: Optional[int]= 5) -> List:
    """
    Collects player ids from each team page.

    Args:
        club_link_dict (Dict[str]): Dictionary with club links as value
        call_rate (int, optional): Number of API calls per minute. Defaults to 5


    Returns:
        List: List of fbref IDs
    """
    
    # Setup
    if call_rate > 10:
        raise FBRefRateLimitError("Call rate is higher than 10")
    
    wait_time = 60 / call_rate
    player_urls = []
    teams_df = pd.read_csv(f"/usr/local/airflow/include/data/results/{season}/teams.csv")
    teams_list = teams_df["name"].to_list()
    
    try:
        # loop over each club name
        for team in tqdm(teams_list, desc= f"Getting {season} Players"):
            link = club_link_dict[team]
            team_players_list = get_url_list(link)
            player_urls.extend(team_players_list)
            
            time.sleep(wait_time)
            
        # transform urls to ids
        fbref_ids = get_ids(player_urls)
        
        return fbref_ids
    
    except Exception as e:
        raise Exception(str(e))
    

def main():
    """
    Main function to orchestrate the data collection process.
    """
    try:
        # Setup
        clubs_link_dict = {
        "Liverpool": "https://fbref.com/en/squads/822bd0ba/2024-2025/all_comps/Liverpool-Stats-All-Competitions",
        "Manchester City": "https://fbref.com/en/squads/b8fd03ef/2024-2025/all_comps/Manchester-City-Stats-All-Competitions",
        "Arsenal": "https://fbref.com/en/squads/18bb7c10/2024-2025/all_comps/Arsenal-Stats-All-Competitions",
        "Everton": "https://fbref.com/en/squads/cff3d9bb/2024-2025/all_comps/Chelsea-Stats-All-Competitions",
        "Aston Villa": "https://fbref.com/en/squads/8602292d/2024-2025/all_comps/Aston-Villa-Stats-All-Competitions",
        "Brighton": "https://fbref.com/en/squads/d07537b9/2024-2025/all_comps/Brighton-and-Hove-Albion-Stats-All-Competitions",
        "Newcastle": "https://fbref.com/en/squads/b2b47a98/2024-2025/all_comps/Newcastle-United-Stats-All-Competitions",
        "Fulham": "https://fbref.com/en/squads/fd962109/2024-2025/all_comps/Fulham-Stats-All-Competitions",
        "Spurs": "https://fbref.com/en/squads/361ca564/2024-2025/all_comps/Tottenham-Hotspur-Stats-All-Competitions",
        "Nottingham Forest": "https://fbref.com/en/squads/e4a775cb/2024-2025/all_comps/Nottingham-Forest-Stats-All-Competitions",
        "Brentford": "https://fbref.com/en/squads/cd051869/2024-2025/all_comps/Brentford-Stats-All-Competitions",
        "West Ham": "https://fbref.com/en/squads/7c21e445/2024-2025/all_comps/West-Ham-United-Stats-All-Competitions",
        "Bournemouth": "https://fbref.com/en/squads/4ba7cbea/2024-2025/all_comps/Bournemouth-Stats-All-Competitions",
        "Manchester United": "https://fbref.com/en/squads/19538871/2024-2025/all_comps/Manchester-United-Stats-All-Competitions",
        "Leicester": "https://fbref.com/en/squads/a2d435b3/2024-2025/all_comps/Leicester-City-Stats-All-Competitions",
        "Everton": "https://fbref.com/en/squads/d3fd31cc/2024-2025/all_comps/Everton-Stats-All-Competitions",
        "Ipswich": "https://fbref.com/en/squads/b74092de/2024-2025/all_comps/Ipswich-Town-Stats-All-Competitions",
        "Crystal Palace": "https://fbref.com/en/squads/47c64c55/2024-2025/all_comps/Crystal-Palace-Stats-All-Competitions",
        "Southampton": "https://fbref.com/en/squads/33c895d4/2024-2025/all_comps/Southampton-Stats-All-Competitions",
        "Wolves": "https://fbref.com/en/squads/8cec06e1/2024-2025/all_comps/Wolverhampton-Wanderers-Stats-All-Competitions"
        }
        
        df = pd.read_csv("./data/results/2024-25/player_idlist.csv")
        fpl_ids = df["id"]
        fpl_names = f"{df['first_name']} {df['second_name']}"
        
        # Collect ids
        fbref_ids = collect_team_players(clubs_link_dict)
        
        
        with open("./data/results/2024-25/fbref_ids.csv", "w+") as file_out:
            w = csv.DictWriter(file_out, ["name", "id"])
            w.writeheader()
            
            w.writerows(fbref_ids)
    
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        return 1

if __name__ == "__main__":
    exit(main())