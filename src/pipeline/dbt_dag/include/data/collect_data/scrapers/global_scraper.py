"""
Fantasy Premier League (FPL) Global Data Scraper

This script orchestrates the collection, parsing, and processing of FPL data for a given season.
It handles multiple data types including player data, fixtures, team data, and gameweek information.

Dependencies:
    - getters: FPL API data retrieval functions
    - collectors: Data collection utilities
    - parsers: Data parsing functions
    - cleaners: Data cleaning utilities
"""

import os
import sys
sys.path.insert(0, os.path.abspath('.'))

import time
from typing import Dict, Any, Optional
from include.data.collect_data.processors.getters import *
from include.data.collect_data.processors.collectors import *
from include.data.collect_data.processors.parsers import *
from include.data.collect_data.processors.cleaners import *
import re
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
class ScraperError(Exception):
    """Base exception class for scraper-related errors."""
    pass

def time_operation(operation_name: str, operation_func: callable, *args, **kwargs) -> Any:
    """
    Execute and time an operation, printing its status.

    Args:
        operation_name (str): Name of the operation for display
        operation_func (callable): Function to execute
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        Any: Result of the operation function

    Raises:
        ScraperError: If operation fails
    """
    print(f"{operation_name}...", end=" ")
    start = time.time()
    
    try:
        result = operation_func(*args, **kwargs)
        elapsed_time = time.time() - start
        print(f"DONE ({elapsed_time:.2f} seconds)")
        return result
    
    except Exception as e:
        elapsed_time = time.time() - start
        print(f"FAILED ({elapsed_time:.2f} seconds)")
        raise ScraperError(f"Error during {operation_name}: {str(e)}")

def global_scraper(season: str) -> None:
    """
    Execute global scraping operation for FPL data.

    Args:
        season (str): Season identifier (e.g., "2024-25")

    Raises:
        ScraperError: If any scraping operation fails
        ValueError: If season format is invalid
    """
    try:
        # Validate season format
        if not re.match(r"^\d{4}-\d{2}$", season):
            raise ValueError("Season must be in format 'YYYY-YY'")

        print("Scraping Data\n")
        overall_start_time = time.time()
        
        # Setup base path and ensure directory exists
        base_path = f"/usr/local/airflow/include/data/results/{season}/"
        os.makedirs(base_path, exist_ok=True)
        
        # Get and parse main FPL data
        data = time_operation("Getting data", get_data)
        
        time_operation("Parsing Data", parse_players, data["elements"], base_path)
        
        # Find current gameweek
        gw_num = 0
        for event in data["events"]:
            if event["is_current"]:
                gw_num = event["id"]
                break
        
        # Clean player data
        time_operation("Cleaning Summary Data", clean_players, "players_raw.csv", base_path)
        
        # Get and parse fixture data
        fixture_data = time_operation("Getting Fixtures Data", get_fixture_data)
        time_operation("Parsing Fixtures", parse_fixtures, fixture_data, base_path)
        
        # Process team data
        team_data = data["teams"]
        time_operation("Processing Team Data", parse_team_data, team_data, base_path, season)
        
        # Process player data
        start = time.time()
        
        try:
            # Setup player directories
            player_base_path = os.path.join(base_path, 'players')
            gw_base_path = os.path.join(base_path, 'gws')
            os.makedirs(player_base_path, exist_ok=True)
            os.makedirs(gw_base_path, exist_ok=True)
            
            # Process player IDs
            time_operation("Identifying Players", id_players, "players_raw.csv", base_path)
            player_ids = get_player_ids(base_path)
            
            # Process individual player data
            print("Processing Players...", end= "")
            for id, name in player_ids.items():
                try:
                    player_data = get_individual_data(id)
                    parse_player_season_history(player_data["history_past"], player_base_path, name, id)
                    parse_player_gw_history(player_data["history"], player_base_path, name, id)
                
                except Exception as e:
                    print(f"Warning: Failed to process player {name} (ID: {id}): {str(e)}")
                    continue
            
            elapsed_time = time.time() - start
            print(f"DONE ({elapsed_time:.2f} seconds)")
            
        except Exception as e:
            raise ScraperError(f"Error processing player data: {str(e)}")
        
        # Process gameweek data if available
        if gw_num > 0:
            time_operation("Getting GW Data", collect_gw, base_path, gw_base_path, gw_num)
            
            try:
                time_operation("Collecting All GWs", collect_all_gws, base_path, gw_base_path, gw_num)
                time_operation("Merging Gameweeks", merge_all_gameweeks, base_path, 1, gw_num)
            
            except Exception as e:
                raise ScraperError(f"Error processing gameweek data: {str(e)}")
        
        overall_elapsed_time = time.time() - overall_start_time
        print(f"\nGlobal scraper completed in {overall_elapsed_time:.2f} seconds.")
        
    except ValueError as e:
        print(f"Error: Invalid input - {str(e)}")
        raise
        
    except ScraperError as e:
        print(f"Error: Scraping failed - {str(e)}")
        raise
        
    except Exception as e:
        print(f"Error: Unexpected error occurred - {str(e)}")
        raise

def main():
    try:
        season = "2024-25"
        global_scraper(season)
    
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        return 1
    
if __name__ == "__main__":
    exit(main())