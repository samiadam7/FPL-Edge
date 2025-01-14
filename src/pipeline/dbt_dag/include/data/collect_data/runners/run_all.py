import os
import sys
sys.path.insert(0, os.path.abspath('.'))

import re
import logging
import csv
import pandas as pd
from pathlib import Path
import time
from typing import Optional, Dict, List
import datetime
from urllib.parse import urlparse
import numpy as np

from include.data.collect_data.scrapers.global_scraper import global_scraper
from include.data.collect_data.scrapers.fbref_get_ids import collect_team_players
from include.data.collect_data.processors.merge_ids import load_dfs, fuzzy_match, map_name_match,\
    sift_names, manual_sift, process_player_matches, save_data
from include.data.collect_data.scrapers.fbref_get_data import collect_players_data
from include.data.collect_data.processors.cleaners import clean_players

def get_current_season() -> str:
    """Returns the current season in the format 'YYYY-YY'."""
    now = datetime.datetime.now()
    current_year = now.year
    if now.month >= 8:
        next_year_short = str(current_year + 1)[-2:]
        return f"{current_year}-{next_year_short}"
    else:
        prev_year = current_year - 1
        current_year_short = str(current_year)[-2:]
        return f"{prev_year}-{current_year_short}"

def get_previous_season(season_str: str) -> str:
    # Split the season string into start and end years
    start_year = int(season_str[:4])
    end_year = int(season_str[5:])
    
    # Subtract 1 from both years
    previous_start_year = start_year - 1
    previous_end_year = end_year - 1
    
    # Format the previous season in "YYYY-YY" format
    return f"{previous_start_year}-{str(previous_end_year)[-2:]}"

# Global vars

CURRENT_SEASON = get_current_season()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def collect_global_scraper(season: str) -> None:
    try:
        global_scraper(season)
        logging.info("Global scraper data collection completed.")

    except Exception as e:
        logging.error(f"Error in global scraper: {e}")
        raise

def parse_season(season_str: str) -> tuple:
    """
    Convert a season string 'YYYY-YY' into a tuple of integers (start_year, end_year).
    Example: '2020-21' -> (2020, 2021)
    """
    start_year = int(season_str[:4])
    end_year = int(season_str[5:7]) + 2000  # Adjust end year to full year format
    return (start_year, end_year)

def load_previous_fbref_ids(current_season: str, all_seasons: List[str]) -> pd.DataFrame:
    # Define the list of previous seasons
    all_seasons = ["2020-21", "2021-22", "2022-23", "2023-24"]  # Adjust according to your seasons
    
    # Parse the current season for comparison
    current_season_tuple = parse_season(current_season)
    
    # Load fbref_ids from all seasons prior to the current season
    previous_fbref_ids = pd.DataFrame(columns=["name", "id"])  # Define the correct columns
    for season in all_seasons:
        # Parse each season string
        season_tuple = parse_season(season)
        
        # Only consider seasons that are strictly earlier than the current season
        if season_tuple <= current_season_tuple:
            try:
                season_path = f"/usr/local/airflow/include/data/results/{season}/fbref_ids.csv"
                fbref_ids = pd.read_csv(season_path)
                previous_fbref_ids = pd.concat([previous_fbref_ids, fbref_ids], ignore_index=True)
            
            except FileNotFoundError:
                logging.warning(f"fbref_ids.csv not found for season {season}")
    
    return previous_fbref_ids.drop_duplicates(subset='name', keep='first')

def collect_fbref_ids(clubs_link_dict: Dict[str,str], season: str) -> None:
    try:
        
        fbref_ids = collect_team_players(clubs_link_dict, season)
        
        with open(f"/usr/local/airflow/include/data/results/{season}/fbref_ids.csv", "w+") as file_out:
            w = csv.DictWriter(file_out, ["name", "id"])
            w.writeheader()
            
            w.writerows(fbref_ids)
    
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        raise

def perform_fuzzy_matching_and_sifting(unmatched: pd.DataFrame, fbref_ids: pd.DataFrame) -> pd.DataFrame:
    try:
        # Perform fuzzy matching
        logging.info("Performing fuzzy matching...")
        threshold = 95
        unmatched.loc[:, 'fuzzy_match'] = unmatched['full_name_abbr'].apply(
            lambda name: fuzzy_match(name, fbref_ids['name'].tolist(), threshold)
        )
        unmatched = map_name_match(unmatched, fbref_ids)
        
        # Sifting names
        logging.info("Strict Last Sifting...")
        unmatched = sift_names(unmatched, fbref_ids, "strict", "last")
        time.sleep(5)
        
        logging.info("Loose First Sifting...")
        unmatched = sift_names(unmatched, fbref_ids, "loose", "first")
        time.sleep(5)
        
        logging.info("Manual Sifting...")
        unmatched = manual_sift(unmatched)
        
        return unmatched

    except Exception as e:
        logging.error(f"Error matching and sifting: {e}")
        raise

def merge_fpl_fbref_ids(season: str) -> None:
    try:
        # Define file paths
        fbref_path = f"/usr/local/airflow/include/data/results/{season}/fbref_ids.csv"
        fpl_path = f'/usr/local/airflow/include/data/results/{season}/player_idlist.csv'
        
        # Load and preprocess data
        logging.info("Loading player data...")
        fbref_ids, fpl_ids = load_dfs(fbref_path, fpl_path)
        
        # Merge datasets
        merged = pd.merge(
            fpl_ids, 
            fbref_ids, 
            "left", 
            on=["first_name", "second_name"], 
            suffixes=["_fpl", "_fbref"]
        )
        
        try: # <- TODO: get previous season player's name
            prev_season = get_previous_season(season)
            previous_merged_ids = pd.read_csv(f"/usr/local/airflow/include/data/results/{prev_season}/player_compiled_ids.csv")
            
            # Merge the current data with the previous season based on first_name and second_name
            merged_with_prev = pd.merge(
                merged, 
                previous_merged_ids[["first_name_fpl", "second_name_fpl", "id_fbref"]], 
                how="left", 
                left_on=["first_name", "second_name"], 
                right_on=["first_name_fpl", "second_name_fpl"], 
                suffixes=("", "_prev")
            )
            
            # Update the id_fbref in the merged dataframe if it is missing (i.e., NaN) in the current season
            merged_with_prev["id_fbref"] = merged_with_prev["id_fbref"].combine_first(merged_with_prev["id_fbref_prev"])
            merged_with_prev.drop(columns=["first_name_fpl", "second_name_fpl", "id_fbref_prev"], inplace= True)
            
            merged = merged_with_prev
            
        except FileNotFoundError:
            logging.warning(f"No previous season data found for {prev_season}. Proceeding without matching.")

        # Split into matched and unmatched players
        unmatched = merged[merged["id_fbref"].isna()].copy()
        matched = merged[~merged["id_fbref"].isna()].drop(
            columns=[col for col in merged.columns
                    if col not in ["first_name", "second_name", "name", "id_fpl", "id_fbref"]]
        )
        
        logging.info(f"Direct matches found: {len(matched)} \n")
        unmatched = perform_fuzzy_matching_and_sifting(unmatched, fbref_ids)
        
        # Process final results
        logging.info("Processing results...")
        final_matched, missing_players = process_player_matches(unmatched, matched)
        
        # Save results
        logging.info("Saving results...")
        save_data(final_matched, missing_players, season)
        
        logging.info("Processing completed successfully")
        
    except Exception as e:
        logging.error(f"Error in matching fbref and fpl IDs: {e}")
        raise
    
def collect_fbref_data(season: str) -> None:
    try:
        # Setup paths
        input_path = Path(f"/usr/local/airflow/include/data/results/{season}/player_compiled_ids.csv")
        output_path = Path(f"/usr/local/airflow/include/data/results/{season}/fbref_merged_gw_data.csv")
        
        # Ensure directories exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Read player data
        players = pd.read_csv(input_path)
        logging.info(f"Found {len(players)} players to process")
        
        # Collect player data
        fbref_season = season[:5] + "20" + season[5:]
        df_list = collect_players_data(
            players,
            fbref_season,
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
        
        merged_df = pd.concat(df_list, axis=0, ignore_index=True, join="outer")
        merged_df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved data for {len(players)} players")
            
    except Exception as e:
        logging.error(f"Error in main process: {e}")

def fpl_data_past_season(season: str) -> None:
    if not isinstance(season, str):
        raise ValueError("Season must be a string")
    if not re.match(r'^\d{4}-\d{2}$', season):
        raise ValueError("Season must be in the format 'YYYY-YY'")
    
    try:
        base_path = f"/usr/local/airflow/include/data/results/{season}/"
        os.makedirs(base_path, exist_ok=True)
        
        base_url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/"
        files_needed = ["fixtures.csv", "player_idlist.csv", "teams.csv", "gws/merged_gw.csv", "players_raw.csv"]
        
        for file in files_needed:
            url = base_url + file
            df = pd.read_csv(url)
                
            if file.__contains__("/"):
                out_filename = file.split("/")[-1]
                
            else:
                out_filename = file
            
            df.to_csv(base_path + out_filename, index= False)
    
    except Exception as e:
        logging.error(f"Error in main process: {e}")

def pull_and_collect_fpl_data(season: str):
        if season == CURRENT_SEASON:
            collect_global_scraper(season)

        else:
            fpl_data_past_season(season)
            clean_players("players_raw.csv", f"data/{season}/")
            
def pull_and_collect_fbref_data(season: str,
                                clubs_link_dict: Dict[str, str],
                                collect_idx: Optional[bool]= True):
    if collect_idx:
        collect_fbref_ids(clubs_link_dict, season)
        merge_fpl_fbref_ids(season)
    
    collect_fbref_data(season)
    
def final_csv_edits(season: str, clubs_link_dict):
    try:
        teams = pd.read_csv(f"/usr/local/airflow/include/data/results/{season}/teams.csv")
        teams["season"] = season
        teams["fbref_id"] = teams["name"].map(clubs_link_dict).apply(lambda x: urlparse(x).path.split("/")[3])
        teams.to_csv(f"/usr/local/airflow/include/data/results/{season}/teams.csv", index= False)
        
        fixtures = pd.read_csv(f"/usr/local/airflow/include/data/results/{season}/fixtures.csv")
        fixtures["season"] = season
        fixtures.to_csv(f"/usr/local/airflow/include/data/results/{season}/fixtures.csv", index= False)
        
        players_clean = pd.read_csv(f"/usr/local/airflow/include/data/results/{season}/players_clean.csv")
        players_clean["season"] = season
        players_clean.to_csv(f"/usr/local/airflow/include/data/results/{season}/players_clean.csv", index= False)
        
        merged_gw = pd.read_csv(f"/usr/local/airflow/include/data/results/{season}/merged_gw.csv")
        merged_gw["season"] = season
        if 'modified' in merged_gw.columns:
            merged_gw.drop(columns= "modified", inplace= True)
        merged_gw.to_csv(f"/usr/local/airflow/include/data/results/{season}/merged_gw.csv", index= False)
        
        fbref_merged_gw = pd.read_csv(f"/usr/local/airflow/include/data/results/{season}/fbref_merged_gw_data.csv")
        fbref_merged_gw = fbref_merged_gw[~fbref_merged_gw["Round"].str.contains("League")]
        fbref_merged_gw["Round"] = fbref_merged_gw["Round"].str.split(" ").str[-1].astype(int)
        fbref_merged_gw.replace("On matchday squad, but did not play", np.NaN, inplace= True)
        fbref_merged_gw["Season"] = season
        fbref_merged_gw["Captain"] = np.where(fbref_merged_gw["Start"] == "Y*", 1, 0)
        start_map = {"Y": 1, "N": 0}
        fbref_merged_gw["Start"] = fbref_merged_gw["Start"].replace("Y*", "Y")
        fbref_merged_gw["Start"] = fbref_merged_gw["Start"].map(start_map)
        
        fbref_merged_gw.to_csv(f"/usr/local/airflow/include/data/results/{season}/fbref_merged_gw_data.csv", index= False)
    
    except FileNotFoundError as e:
        logging.error(f"File was not found: {e}")
        raise
    
    except Exception as e:
        logging.error(f"Error in final csv edits: {e}")
        raise

def run_all(season: str, clubs_link_dict: Dict[str,str], collect_idx: Optional[bool]= True) -> None:
    if not isinstance(season, str):
        raise ValueError("Season must be a string")
    if not re.match(r'^\d{4}-\d{2}$', season):
        raise ValueError("Season must be in the format 'YYYY-YY'")
    
    if not isinstance(collect_idx, bool):
        raise ValueError("Collect_idx must be a boolean")
    
    try:
        logging.info("Starting full data collection pipeline...")
        
        pull_and_collect_fpl_data(season)
        pull_and_collect_fbref_data(season, clubs_link_dict, collect_idx)
        
        # edits to csvs
        final_csv_edits(season, clubs_link_dict) # <- For some reason not working
        
        logging.info("Data collection pipeline completed successfully.")
            
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

def generate_clubs_link_dict(clubs_link_dict_template: Dict[str, str], fbref_season: str) -> Dict[str, str]:
    return {club: link.replace("{fbref_season}", fbref_season)
            for club, link in clubs_link_dict_template.items()}

def collect_all_seasons_idx(seasons: List[str], clubs_link_dict_template: Dict[str, str]):
    if not isinstance(seasons, list):
        raise ValueError("Seasons must be a list containing strings in the format ")
    if not all(isinstance(season, str) and re.match(r'^\d{4}-\d{2}$', season) for season in seasons):
        raise ValueError("Seasons entries must be strings in the format 'YYYY-YY'")
    
    if not isinstance(clubs_link_dict_template, dict):
        raise ValueError("clubs_link_dict must be a dictionary")
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in clubs_link_dict_template.items()):
        raise ValueError("All keys and values in clubs_link_dict must be strings")
    
    try:
        for season in seasons:
            fbref_season = season[:5] + "20" + season[5:]
            clubs_link_dict = generate_clubs_link_dict(clubs_link_dict_template, fbref_season)
            
            pull_and_collect_fpl_data(season)
            collect_fbref_ids(clubs_link_dict, season)
            
        for season in seasons:
            logging.info(f"Merging fpl and fbref data for {season} Season")
            merge_fpl_fbref_ids(season)
            
    except Exception as e:
        logging.error(f"Failed to collect all seasons ids: {e}")

def run_all_seasons(seasons: List[str],
                    clubs_link_dict_template: Dict[str, str],
                    collect_idx: Optional[bool]= True) -> None:
    if not isinstance(seasons, list):
        raise ValueError("Seasons must be a list")
    if not all(isinstance(season, str) for season in seasons):
        raise ValueError("Seasons must contain strings in the format YYYY-YY")
    
    if not isinstance(clubs_link_dict_template, dict):
        raise ValueError("clubs_link_dict must be a dictionary")
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in clubs_link_dict_template.items()):
        raise ValueError("All keys and values in clubs_link_dict must be strings")
    
    if not isinstance(collect_idx, bool):
        raise ValueError("collect_idx must be a boolean")
    
    try:
        if collect_idx == True:
            collect_all_seasons_idx(seasons, clubs_link_dict_template)
                              
        for season in seasons:
            fbref_season = season[:5] + "20" + season[5:]
            clubs_link_dict = generate_clubs_link_dict(clubs_link_dict_template, fbref_season)
            run_all(season, clubs_link_dict, False) # If you want to collect all, set collect_idx to true
            
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

def collect_recent_gw():
    clubs_link_dict_template = {
        "Liverpool": "https://fbref.com/en/squads/822bd0ba/{fbref_season}/all_comps/Liverpool-Stats-All-Competitions",
        "Man City": "https://fbref.com/en/squads/b8fd03ef/{fbref_season}/all_comps/Manchester-City-Stats-All-Competitions",
        "Arsenal": "https://fbref.com/en/squads/18bb7c10/{fbref_season}/all_comps/Arsenal-Stats-All-Competitions",
        "Chelsea": "https://fbref.com/en/squads/cff3d9bb/{fbref_season}/all_comps/Chelsea-Stats-All-Competitions",
        "Aston Villa": "https://fbref.com/en/squads/8602292d/{fbref_season}/all_comps/Aston-Villa-Stats-All-Competitions",
        "Brighton": "https://fbref.com/en/squads/d07537b9/{fbref_season}/all_comps/Brighton-and-Hove-Albion-Stats-All-Competitions",
        "Newcastle": "https://fbref.com/en/squads/b2b47a98/{fbref_season}/all_comps/Newcastle-United-Stats-All-Competitions",
        "Fulham": "https://fbref.com/en/squads/fd962109/{fbref_season}/all_comps/Fulham-Stats-All-Competitions",
        "Spurs": "https://fbref.com/en/squads/361ca564/{fbref_season}/all_comps/Tottenham-Hotspur-Stats-All-Competitions",
        "Nott'm Forest": "https://fbref.com/en/squads/e4a775cb/{fbref_season}/all_comps/Nottingham-Forest-Stats-All-Competitions",
        "Brentford": "https://fbref.com/en/squads/cd051869/{fbref_season}/all_comps/Brentford-Stats-All-Competitions",
        "West Ham": "https://fbref.com/en/squads/7c21e445/{fbref_season}/all_comps/West-Ham-United-Stats-All-Competitions",
        "Bournemouth": "https://fbref.com/en/squads/4ba7cbea/{fbref_season}/all_comps/Bournemouth-Stats-All-Competitions",
        "Man Utd": "https://fbref.com/en/squads/19538871/{fbref_season}/all_comps/Manchester-United-Stats-All-Competitions",
        "Leicester": "https://fbref.com/en/squads/a2d435b3/{fbref_season}/all_comps/Leicester-City-Stats-All-Competitions",
        "Everton": "https://fbref.com/en/squads/d3fd31cc/{fbref_season}/all_comps/Everton-Stats-All-Competitions",
        "Ipswich": "https://fbref.com/en/squads/b74092de/{fbref_season}/all_comps/Ipswich-Town-Stats-All-Competitions",
        "Crystal Palace": "https://fbref.com/en/squads/47c64c55/{fbref_season}/all_comps/Crystal-Palace-Stats-All-Competitions",
        "Southampton": "https://fbref.com/en/squads/33c895d4/{fbref_season}/all_comps/Southampton-Stats-All-Competitions",
        "Wolves": "https://fbref.com/en/squads/8cec06e1/{fbref_season}/all_comps/Wolverhampton-Wanderers-Stats-All-Competitions",
        "Burnley": "https://fbref.com/en/squads/943e8050/{fbref_season}/all_comps/Burnley-Stats-All-Competitions",
        "Luton": "https://fbref.com/en/squads/e297cd13/{fbref_season}/all_comps/Luton-Town-Stats-All-Competitions",
        "Sheffield Utd": "https://fbref.com/en/squads/1df6b87e/{fbref_season}/all_comps/Sheffield-United-Stats-All-Competitions",
        "Leeds": "https://fbref.com/en/squads/5bfb9659/{fbref_season}/all_comps/Leeds-United-Stats-All-Competitions",
        "Norwich": "https://fbref.com/en/squads/1c781004/{fbref_season}/all_comps/Norwich-City-Stats-All-Competitions",
        "Watford": "https://fbref.com/en/squads/2abfe087/{fbref_season}/all_comps/Watford-Stats-All-Competitions",
        "West Brom": "https://fbref.com/en/squads/60c6b05f/{fbref_season}/all_comps/West-Bromwich-Albion-Stats-All-Competitions"
        }

    curr_szn = get_current_season()
    run_all(curr_szn, clubs_link_dict_template, False)
    
    # fpl_merged_gw = pd.read_csv(f"/usr/local/airflow/include/data/results/{curr_szn}/merged_gw.csv")
    # fpl_recent_gw = fpl_merged_gw[fpl_merged_gw["GW"] == fpl_merged_gw["GW"].max()]
    # fpl_recent_gw.to_csv(f"/usr/local/airflow/include/data/results/{curr_szn}/most_recent_gw.csv", index= False)
    
    # fbref_merged_gw = pd.read_csv(f"/usr/local/airflow/include/data/results/{curr_szn}/fbref_merged_gw_data.csv")
    # fbref_recent_gw = fbref_merged_gw[fbref_merged_gw["Round"] == fbref_merged_gw["Round"].max()]
    # fbref_recent_gw.to_csv(f"/usr/local/airflow/include/data/results/{curr_szn}/fbref_most_recent_gw.csv", index= False)

def main():
    clubs_link_dict_template = {
        "Liverpool": "https://fbref.com/en/squads/822bd0ba/{fbref_season}/all_comps/Liverpool-Stats-All-Competitions",
        "Man City": "https://fbref.com/en/squads/b8fd03ef/{fbref_season}/all_comps/Manchester-City-Stats-All-Competitions",
        "Arsenal": "https://fbref.com/en/squads/18bb7c10/{fbref_season}/all_comps/Arsenal-Stats-All-Competitions",
        "Chelsea": "https://fbref.com/en/squads/cff3d9bb/{fbref_season}/all_comps/Chelsea-Stats-All-Competitions",
        "Aston Villa": "https://fbref.com/en/squads/8602292d/{fbref_season}/all_comps/Aston-Villa-Stats-All-Competitions",
        "Brighton": "https://fbref.com/en/squads/d07537b9/{fbref_season}/all_comps/Brighton-and-Hove-Albion-Stats-All-Competitions",
        "Newcastle": "https://fbref.com/en/squads/b2b47a98/{fbref_season}/all_comps/Newcastle-United-Stats-All-Competitions",
        "Fulham": "https://fbref.com/en/squads/fd962109/{fbref_season}/all_comps/Fulham-Stats-All-Competitions",
        "Spurs": "https://fbref.com/en/squads/361ca564/{fbref_season}/all_comps/Tottenham-Hotspur-Stats-All-Competitions",
        "Nott'm Forest": "https://fbref.com/en/squads/e4a775cb/{fbref_season}/all_comps/Nottingham-Forest-Stats-All-Competitions",
        "Brentford": "https://fbref.com/en/squads/cd051869/{fbref_season}/all_comps/Brentford-Stats-All-Competitions",
        "West Ham": "https://fbref.com/en/squads/7c21e445/{fbref_season}/all_comps/West-Ham-United-Stats-All-Competitions",
        "Bournemouth": "https://fbref.com/en/squads/4ba7cbea/{fbref_season}/all_comps/Bournemouth-Stats-All-Competitions",
        "Man Utd": "https://fbref.com/en/squads/19538871/{fbref_season}/all_comps/Manchester-United-Stats-All-Competitions",
        "Leicester": "https://fbref.com/en/squads/a2d435b3/{fbref_season}/all_comps/Leicester-City-Stats-All-Competitions",
        "Everton": "https://fbref.com/en/squads/d3fd31cc/{fbref_season}/all_comps/Everton-Stats-All-Competitions",
        "Ipswich": "https://fbref.com/en/squads/b74092de/{fbref_season}/all_comps/Ipswich-Town-Stats-All-Competitions",
        "Crystal Palace": "https://fbref.com/en/squads/47c64c55/{fbref_season}/all_comps/Crystal-Palace-Stats-All-Competitions",
        "Southampton": "https://fbref.com/en/squads/33c895d4/{fbref_season}/all_comps/Southampton-Stats-All-Competitions",
        "Wolves": "https://fbref.com/en/squads/8cec06e1/{fbref_season}/all_comps/Wolverhampton-Wanderers-Stats-All-Competitions",
        "Burnley": "https://fbref.com/en/squads/943e8050/{fbref_season}/all_comps/Burnley-Stats-All-Competitions",
        "Luton": "https://fbref.com/en/squads/e297cd13/{fbref_season}/all_comps/Luton-Town-Stats-All-Competitions",
        "Sheffield Utd": "https://fbref.com/en/squads/1df6b87e/{fbref_season}/all_comps/Sheffield-United-Stats-All-Competitions",
        "Leeds": "https://fbref.com/en/squads/5bfb9659/{fbref_season}/all_comps/Leeds-United-Stats-All-Competitions",
        "Norwich": "https://fbref.com/en/squads/1c781004/{fbref_season}/all_comps/Norwich-City-Stats-All-Competitions",
        "Watford": "https://fbref.com/en/squads/2abfe087/{fbref_season}/all_comps/Watford-Stats-All-Competitions",
        "West Brom": "https://fbref.com/en/squads/60c6b05f/{fbref_season}/all_comps/West-Bromwich-Albion-Stats-All-Competitions"
        }

    curr_szn = get_current_season()
    run_all(curr_szn, clubs_link_dict_template, True)

if __name__ == "__main__":
    main()