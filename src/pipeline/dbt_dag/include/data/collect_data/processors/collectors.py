"""
FPL Data Collection Module

This module provides functionality for collecting and processing Fantasy Premier League (FPL) gameweek data.
It handles team information, fixtures, player positions, and combines them into gameweek-specific CSV files.
"""

import csv
import os
from typing import Dict, Tuple, List, Optional

class GameweekMergeError(Exception):
    """Custom exception for gameweek merging errors."""
    pass

def get_teams(directory: str) -> Dict[int, str]:
    """
    Load team data from a CSV file and create a mapping of team IDs to team names.
    
    Args:
        directory (str): Directory containing the teams.csv file
        
    Returns:
        Dict[int, str]: Dictionary mapping team IDs to team names
        
    Raises:
        FileNotFoundError: If teams.csv is not found
    """
    teams = {}
    path = os.path.join(directory, "teams.csv")
    
    with open(path, "r") as file_in:
        reader = csv.DictReader(file_in)
        for row in reader:
            teams[int(row["id"])] = row["name"]
    
    return teams

def get_fixtures(directory: str) -> Tuple[Dict[int, int], Dict[int, int]]:
    """
    Load fixture data and create mappings for home and away teams.
    
    Args:
        directory (str): Directory containing the fixtures.csv file
        
    Returns:
        Tuple[Dict[int, int], Dict[int, int]]: Two dictionaries:
            - First dict maps fixture IDs to home team IDs
            - Second dict maps fixture IDs to away team IDs
            
    Raises:
        FileNotFoundError: If fixtures.csv is not found
    """
    fix_home = {}
    fix_away = {}
    
    with open(os.path.join(directory, "fixtures.csv"), "r") as file_in:
        reader = csv.DictReader(file_in)
        for row in reader:
            fix_home[int(row['id'])] = int(row['team_h'])
            fix_away[int(row['id'])] = int(row['team_a'])
            
    return fix_home, fix_away

def get_positions(directory: str) -> Tuple[Dict[int, str], Dict[int, str]]:
    """
    Load player data and create mappings for player names and positions.
    
    Args:
        directory (str): Directory containing the players_raw.csv file
        
    Returns:
        Tuple[Dict[int, str], Dict[int, str]]: Two dictionaries:
            - First dict maps player IDs to full names
            - Second dict maps player IDs to positions
            
    Raises:
        FileNotFoundError: If players_raw.csv is not found
    """
    positions = {}
    names = {}
    pos_dict = {
        '1': "GK",   # Goalkeeper
        '2': "DEF",  # Defender
        '3': "MID",  # Midfielder
        '4': "FWD"   # Forward
    }
    
    with open(os.path.join(directory, "players_raw.csv"), 'r', encoding="utf-8") as file_in:
        reader = csv.DictReader(file_in)
        for row in reader:
            player_id = int(row['id'])
            positions[player_id] = pos_dict[row['element_type']]
            names[player_id] = f"{row['first_name']} {row['second_name']}"
    
    return names, positions

def collect_gw(base_directory: str, output_dir: str, gw: int) -> None:
    """
    Collect and process data for a specific gameweek, combining player, team, and fixture information.
    
    Args:
        base_directory (str): Base directory containing all input files
        output_dir (str): Directory where output files will be saved
        gw (int): Gameweek number to process
        
    Raises:
        FileNotFoundError: If required input files are not found
    """
    rows: List[Dict] = []
    fieldnames: List[str] = []

    # Load reference data
    fix_home, fix_away = get_fixtures(base_directory)
    teams = get_teams(base_directory)
    player_names, player_positions = get_positions(base_directory)

    # Process player gameweek data
    players_dir = os.path.join(base_directory, "players")
    for root, _, files in os.walk(players_dir):
        if "gw.csv" in files:
            path = os.path.join(root, "gw.csv")
            
            with open(path, "r") as file_in:
                reader = csv.DictReader(file_in)
                fieldnames = reader.fieldnames or []

                for row in reader:
                    if int(row['round']) == gw:
                        # Extract player ID from directory name
                        player_id = int(os.path.basename(root).split('_')[-1])
                        
                        # Add player details
                        fixture = int(row['fixture'])
                        row['name'] = player_names[player_id]
                        row['position'] = player_positions[player_id]
                        
                        # Determine team based on home/away status
                        is_home = row['was_home'].lower() == 'true'
                        row['team'] = teams[fix_home[fixture] if is_home else fix_away[fixture]]
                        
                        rows.append(row)

    # Prepare output
    fieldnames = ['name', 'position', 'team', 'xP'] + fieldnames
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Write processed data to CSV
    output_path = os.path.join(output_dir, f"gw_{gw}.csv")
    with open(output_path, "w+", newline='') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
def collect_all_gws(base_directory: str, output_dir: str, current_gw: int) -> None:
    """
    Process multiple gameweeks of data.
    
    Args:
        base_directory (str): Base directory containing all input files
        output_dir (str): Directory where output files will be saved
        current_gw (int): Up to which gameweek to process (exclusive)
        
    Note:
        Will process gameweeks from 1 to (current_gw - 1)
    """
    for gw in range(1, current_gw):
        collect_gw(base_directory, output_dir, gw)
        

def merge_gameweek(
    gameweek: int,
    directory: str,
    output_filename: Optional[str] = "merged_gw.csv"
) -> None:
    """
    Merge a single gameweek's data into a combined CSV file.
    
    Args:
        gameweek (int): The gameweek number to process
        directory (str): Directory containing the gameweek CSV files
        output_filename (str, optional): Name of the output merged file. Defaults to "merged_gw.csv"
        
    Raises:
        GameweekMergeError: If there's an error processing the gameweek data
        FileNotFoundError: If the gameweek file doesn't exist
    """
    try:
        # Setup file paths
        input_filename = f"gws/gw_{gameweek}.csv"
        input_path = os.path.join(directory, input_filename)
        output_path = os.path.join(directory, output_filename)

        # Read the gameweek data
        rows: List[Dict] = []
        fieldnames: List[str] = []
        
        with open(input_path, 'r', encoding="utf-8") as fin:
            reader = csv.DictReader(fin)
            
            # Validate and get fieldnames
            if not reader.fieldnames:
                raise GameweekMergeError(f"No fields found in {input_filename}")
            fieldnames = reader.fieldnames + ["GW"]
            
            # Process each row
            for row in reader:
                row["GW"] = gameweek
                rows.append(row)

        # Write to the merged file
        write_header = not os.path.exists(output_path) or os.path.getsize(output_path) == 0
        
        with open(output_path, 'a', encoding="utf-8", newline='') as fout:
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            
            if write_header:
                writer.writeheader()
            
            writer.writerows(rows)
            
        # print(f"Successfully merged gameweek {gameweek}")

    except FileNotFoundError:
        raise FileNotFoundError(f"Gameweek file not found: {input_filename}")
    
    except csv.Error as e:
        raise GameweekMergeError(f"CSV processing error in gameweek {gameweek}: {str(e)}")
    
    except Exception as e:
        raise GameweekMergeError(f"Error processing gameweek {gameweek}: {str(e)}")

def merge_all_gameweeks(
    directory: str,
    start_gw: int = 1,
    end_gw: int = 38,
    output_filename: Optional[str] = "merged_gw.csv"
) -> None:
    """
    Merge multiple gameweeks into a single CSV file.
    
    Args:
        directory (str): Directory containing the gameweek CSV files
        start_gw (int, optional): Starting gameweek number. Defaults to 1
        end_gw (int, optional): Ending gameweek number. Defaults to 38
        output_filename (str, optional): Name of the output merged file. Defaults to "merged_gw.csv"
        
    Raises:
        GameweekMergeError: If there's an error during the merging process
    """
    # Delete existing merged file if it exists
    output_path = os.path.join(directory, output_filename)
    if os.path.exists(output_path):
        os.remove(output_path)

    # Process each gameweek
    errors = []
    for gw in range(start_gw, end_gw + 1):
        try:
            merge_gameweek(gw, directory, output_filename)
            
        except (FileNotFoundError, GameweekMergeError) as e:
            errors.append(f"GW{gw}: {str(e)}")
            continue

    # Report any errors that occurred
    if errors:
        error_msg = "\n".join(errors)
        raise GameweekMergeError(f"Errors occurred during merging:\n{error_msg}")


def main():
    try:
        base_dir = "/Users/samiadam/Repositories/Projects/FPL_Project/src/pipeline/dbt_dag/dags/data/results/2024-25"
        output_dir = "/Users/samiadam/Repositories/Projects/FPL_Project/src/pipeline/dbt_dag/dags/data/results/2024-25/gws"
        current_gw = 9
        
        collect_all_gws(base_dir, output_dir, current_gw)
        print("Successfully collected gw data")
        
        directory = "/Users/samiadam/Repositories/Projects/FPL_Project/src/pipeline/dbt_dag/dags/data/results/2024-25/"
        
        merge_all_gameweeks(directory, 1, current_gw)
        print("Successfully merged all gameweeks")
        
        return 0
    
    except Exception as e:
        print(e)
        return 1
    
if __name__ == "__main__":
    exit(main())