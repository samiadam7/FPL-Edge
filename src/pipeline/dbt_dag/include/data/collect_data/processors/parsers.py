"""
Fantasy Premier League (FPL) Data Parser

This module provides functions to parse and save FPL data into CSV files.
It handles various data formats including player statistics, fixtures, and team data.
"""

import pandas as pd
import csv
import os
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FPLParsingError(Exception):
    """Raised when there's an error parsing FPL data."""
    pass

class FPLFileWriteError(Exception):
    """Raised when there's an error writing FPL data to files."""
    pass

def extract_stat_names(stat_dict: Dict[str, Any]) -> List[str]:
    """
    Extract statistical column names from a dictionary.
    
    Args:
        stat_dict (Dict[str, Any]): Dictionary containing statistical data
        
    Returns:
        List[str]: List of column names
        
    Raises:
        FPLParsingError: If stat_dict is empty or invalid
    """
    try:
        if not stat_dict:
            raise ValueError("Empty statistics dictionary provided")

        return list(stat_dict.keys())

    except Exception as e:
        raise FPLParsingError(f"Failed to extract stat names: {str(e)}")

def parse_players(list_of_players: List[Dict[str, Any]], base_path: str) -> None:
    """
    Parse player data and save to CSV.
    
    Args:
        list_of_players (List[Dict[str, Any]]): List of player dictionaries
        base_path (str): Base directory path for saving files
        
    Raises:
        FPLParsingError: If parsing fails
        FPLFileWriteError: If file writing fails
    """
    try:
        if not list_of_players:
            raise ValueError("Empty player list provided")
            
        stat_names = extract_stat_names(list_of_players[0])
        file_name = os.path.join(base_path, "players_raw.csv")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        
        with open(file_name, "w+", encoding="utf8", newline="") as f:
            w = csv.DictWriter(f, sorted(stat_names))
            w.writeheader()
            for player in list_of_players:
                w.writerow({k: str(v).encode('utf-8').decode('utf-8') 
                           for k, v in player.items()})
                           
        # logging.info(f"Successfully wrote player data to {file_name}")
                           
    except OSError as e:
        raise FPLFileWriteError(f"Failed to write player data: {str(e)}")
    
    except Exception as e:
        raise FPLParsingError(f"Failed to parse player data: {str(e)}")
        
def parse_fixtures(data: List[Dict[str, Any]], base_path: str) -> None:
    """
    Parse fixture data and save to CSV.
    
    Args:
        data (List[Dict[str, Any]]): List of fixture dictionaries
        base_path (str): Base directory path for saving files
        
    Raises:
        FPLParsingError: If parsing fails
        FPLFileWriteError: If file writing fails
    """
    try:
        if not data:
            raise ValueError("Empty fixture data provided")
            
        fixtures_df = pd.DataFrame.from_records(data)
        output_path = os.path.join(base_path, "fixtures.csv")
        fixtures_df.to_csv(output_path, index=False)
        # logging.info(f"Successfully wrote fixture data to {output_path}")
        
    except Exception as e:
        raise FPLParsingError(f"Failed to parse fixture data: {str(e)}")
    
def parse_team_data(data: List[Dict[str, Any]], base_path: str, season: str) -> None:
    """
    Parse team data and save to CSV.
    
    Args:
        data (List[Dict[str, Any]]): List of team dictionaries
        base_path (str): Base directory path for saving files
        
    Raises:
        FPLParsingError: If parsing fails
        FPLFileWriteError: If file writing fails
    """
    try:
        if not data:
            raise ValueError("Empty team data provided")
            
        teams_df = pd.DataFrame.from_records(data)
        output_path = os.path.join(base_path, "teams.csv")
        teams_df.to_csv(output_path, index=False)
        # logging.info(f"Successfully wrote team data to {output_path}")
        
    except Exception as e:
        raise FPLParsingError(f"Failed to parse team data: {str(e)}")

def parse_player_gw_history(
    gw_history_list: List[Dict[str, Any]], 
    base_path: str, 
    name: str, 
    id: int
) -> None:
    """
    Parse player gameweek history and save to CSV.
    
    Args:
        gw_history_list (List[Dict[str, Any]]): List of gameweek history dictionaries
        base_path (str): Base directory path for saving files
        name (str): Player name
        id (int): Player ID
        
    Raises:
        FPLParsingError: If parsing fails
        FPLFileWriteError: If file writing fails
    """
    try:
        if not gw_history_list:
            logging.warning(f"No gameweek history for player {name} (ID: {id})")
            return
            
        stat_names = extract_stat_names(gw_history_list[0])
        file_path = os.path.join(base_path, f"{name}_{id}", "gw.csv")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w+", encoding="utf-8", newline="") as file:
            w = csv.DictWriter(file, sorted(stat_names))
            w.writeheader()
            w.writerows(gw_history_list)
            
        # logging.info(f"Successfully wrote gameweek history for player {name} (ID: {id})")
            
    except OSError as e:
        raise FPLFileWriteError(f"Failed to write gameweek history: {str(e)}")
    
    except Exception as e:
        raise FPLParsingError(f"Failed to parse gameweek history: {str(e)}")

def parse_player_season_history(
    player_hist_list: List[Dict[str, Any]], 
    base_path: str, 
    name: str, 
    id: int
) -> None:
    """
    Parse player season history and save to CSV.
    
    Args:
        player_hist_list (List[Dict[str, Any]]): List of season history dictionaries
        base_path (str): Base directory path for saving files
        name (str): Player name
        id (int): Player ID
        
    Raises:
        FPLParsingError: If parsing fails
        FPLFileWriteError: If file writing fails
    """
    try:
        if not player_hist_list:
            logging.warning(f"No season history for player {name} (ID: {id})")
            return
            
        stat_names = extract_stat_names(player_hist_list[0])
        file_path = os.path.join(base_path, f"{name}_{id}", "history.csv")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w+", encoding="utf-8", newline="") as file:
            w = csv.DictWriter(file, sorted(stat_names))
            w.writeheader()
            w.writerows(player_hist_list)
            
        # logging.info(f"Successfully wrote season history for player {name} (ID: {id})")
            
    except OSError as e:
        raise FPLFileWriteError(f"Failed to write season history: {str(e)}")
    except Exception as e:
        raise FPLParsingError(f"Failed to parse season history: {str(e)}")
            
def parse_player_season_history(player_hist_list, base_path, name, id):
    if player_hist_list:
        stat_names = extract_stat_names(player_hist_list[0])
        file_path = os.path.join(base_path, f"{name}_{id}", "history.csv")
        os.makedirs(os.path.dirname(file_path), exist_ok= True)
        
        with open(file_path, "w+", encoding= "utf-8", newline= "") as file:
            w = csv.DictWriter(file, sorted(stat_names))
            w.writeheader()
            w.writerows(player_hist_list)