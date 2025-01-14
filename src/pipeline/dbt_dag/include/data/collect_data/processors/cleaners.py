"""
A module for processing and cleaning Fantasy Premier League (FPL) player data.
This module provides functionality to clean player data, create player ID mappings,
and manage player position classifications.
"""

import csv
from enum import Enum
from pathlib import Path
from typing import Optional
try:
    from typing import Dict
except ImportError:
    from typing_extensions import Dict

class Position(Enum):
    """Enumeration of possible player positions."""
    GK = "1"
    DEF = "2"
    MID = "3"
    FWD = "4"

class PlayerDataError(Exception):
    """Custom exception for player data processing errors."""
    pass

def clean_players(file_name: str, base_path: str, output_file: Optional[str] = None) -> None:
    """
    Clean and transform raw player data into a standardized format.
    
    Args:
        file_name (str): Name of the input CSV file containing raw player data
        base_path (str): Base directory path where input file is located and output will be saved
        output_file (str, optional): Name of output file. Defaults to "players_clean.csv"
    
    Raises:
        PlayerDataError: If there's an issue processing the player data
        FileNotFoundError: If the input file doesn't exist
    """
    headers = ['first_name', 'second_name', 'goals_scored', 'assists', 'total_points', 
               'minutes', 'goals_conceded', 'creativity', 'influence', 'threat', 
               'bonus', 'bps', 'ict_index', 'clean_sheets', 'red_cards', 
               'yellow_cards', 'selected_by_percent', 'now_cost', 'element_type', "team"]

    # Path setup
    base_path = Path(base_path)
    input_path = base_path / file_name
    output_path = base_path / (output_file or "players_clean.csv")

    try:
        with open(input_path, "r", encoding="utf-8") as raw, \
             open(output_path, "w", encoding="utf-8", newline="") as clean:
            
            reader = csv.DictReader(raw)
            writer = csv.DictWriter(clean, headers, extrasaction="ignore")
            writer.writeheader()

            for line in reader:
                try:
                    # Convert position code to enum value
                    position_code = line['element_type']
                    line['element_type'] = Position(position_code).name
                    writer.writerow(line)
                    
                except ValueError:
                    raise PlayerDataError(f"Invalid position code: {position_code}")
    
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {input_path}")

    except csv.Error as e:
        raise PlayerDataError(f"Error processing CSV: {str(e)}")
        
def id_players(file_name: str, base_path: str, output_file: Optional[str] = None) -> None:
    """
    Create a simplified CSV file containing only player identification information.
    
    Args:
        file_name (str): Name of the input CSV file containing player data
        base_path (str): Base directory path where input file is located and output will be saved
        output_file (str, optional): Name of output file. Defaults to "player_idlist.csv"
    
    Raises:
        FileNotFoundError: If the input file doesn't exist
        PlayerDataError: If there's an issue processing the player data
    """
    
    # Setup
    headers = ["first_name", "second_name", "id"]
    
    base_path = Path(base_path)
    input_path = base_path / file_name
    output_path = base_path / (output_file or "player_idlist.csv")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(input_path, "r", encoding="utf-8") as raw, \
             open(output_path, "w", encoding="utf-8", newline="") as clean:
            
            reader = csv.DictReader(raw)
            writer = csv.DictWriter(clean, headers, extrasaction="ignore")
            writer.writeheader()
            
            for line in reader:
                if not all(key in line for key in headers):
                    raise PlayerDataError(f"Missing required fields in data: {headers}")
                
                writer.writerow(line)
                
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {input_path}")
    except csv.Error as e:
        raise PlayerDataError(f"Error processing CSV: {str(e)}")
        
def get_player_ids(base_path: str, file_name: Optional[str] = None) -> Dict[str, str]:
    """
    Create a dictionary mapping player IDs to formatted player names.
    
    Args:
        base_path (str): Base directory path where the player ID list file is located
        file_name (str, optional): Name of input file. Defaults to "player_idlist.csv"
    
    Returns:
        Dict[str, str]: Dictionary mapping player IDs to formatted player names
    
    Raises:
        FileNotFoundError: If the input file doesn't exist
        PlayerDataError: If there's an issue processing the player data
    """
    
    # Setup
    base_path = Path(base_path)
    input_path = base_path / (file_name or "player_idlist.csv")
    player_ids = {}

    try:
        with open(input_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            
            for line in reader:
                try:
                    player_id = int(line["id"])
                    name = (f"{line['first_name']}_{line['second_name'].replace(' ', '_')}")
                    player_ids[player_id] = name
                
                except KeyError as e:
                    raise PlayerDataError(f"Missing required field: {e}")
                
    except FileNotFoundError:
        raise FileNotFoundError(f"Player ID list not found: {input_path}")
    except csv.Error as e:
        raise PlayerDataError(f"Error processing CSV: {str(e)}")

    return player_ids

def main():
    try:
        base_path = "/Users/samiadam/Repositories/Projects/FPL_Project/src/pipeline/dbt_dag/dags/data/results/2024-25/"  # Example base path
        
        # Clean player data
        clean_players("players_raw.csv", base_path)
        
        # Create ID list
        id_players("players_raw.csv", base_path)
        
        # Get player IDs
        player_ids = get_player_ids(base_path)
        print(f"Processed {len(player_ids)} players successfully")
        
    except (FileNotFoundError, PlayerDataError) as e:
        print(f"Error: {e}")
        return 1    # Return code 1 indicates error
    
    return 0        # Return code 0 indicates success

if __name__ == "__main__":
    exit(main())