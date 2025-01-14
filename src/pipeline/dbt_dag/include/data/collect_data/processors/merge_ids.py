"""
Fantasy Premier League (FPL) and FBRef Player ID Mapper

This script handles the mapping between FPL and FBRef player IDs, including:
- Loading and processing player data from both sources
- Matching players using fuzzy matching
- Saving matched and unmatched player data
"""

import json
import pandas as pd
import numpy as np
import unicodedata
from fuzzywuzzy import process
from pathlib import Path
from typing import List, Optional, Tuple, Any, Union
import logging
import time
from urllib.parse import urlparse
# import pyperclip

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataProcessingError(Exception):
    """Raised when there's an error processing player data."""
    pass

class FuzzyMatchError(Exception):
    """Raised when there's an error in fuzzy matching process."""
    pass

def remove_special_letters(text: str) -> str:
    """
    Remove special characters and diacritics from text.
    
    Args:
        text (str): Input text to process
        
    Returns:
        str: Processed text with special characters removed
        
    Raises:
        ValueError: If input is not a string
    """
    try:
        if not isinstance(text, str):
            raise ValueError("Input must be a string")
            
        # Normalize Unicode characters
        normalized_text = unicodedata.normalize('NFKD', text)
        # Remove combining characters
        return ''.join([c for c in normalized_text if not unicodedata.combining(c)])
        
    except Exception as e:
        logging.error(f"Error removing special letters from '{text}': {str(e)}")
        raise

def fuzzy_match(
    name: str, 
    choices: List[str], 
    threshold: int, 
    scorer: callable = process.extractOne) -> Union[str, float]:
    """
    Find the best fuzzy match for a name from a list of choices.
    
    Args:
        name (str): Name to match
        choices (List[str]): List of possible matches
        threshold (int): Minimum score required for a match (0-100)
        scorer (callable): Function to use for scoring matches
        
    Returns:
        Union[str, float]: Best matching name if score >= threshold, else np.NaN
        
    Raises:
        FuzzyMatchError: If matching process fails
    """
    try:
        if not isinstance(name, str):
            raise ValueError("Name must be a string")
        if not isinstance(choices, list) or not all(isinstance(x, str) for x in choices):
            raise ValueError("Choices must be a list of strings")
        if not isinstance(threshold, (int, float)) or not 0 <= threshold <= 100:
            raise ValueError("Threshold must be a number between 0 and 100")
            
        match, score = scorer(name, choices)
        return match if score >= threshold else np.NaN
        
    except Exception as e:
        logging.error(f"Error in fuzzy matching for '{name}': {str(e)}")
        raise FuzzyMatchError(f"Failed to perform fuzzy match: {str(e)}")

def sorted_fuzzy_match(
    name: str, 
    choices: List[str], 
    scorer: callable = process.extract) -> List[Tuple[str, int]]:
    """
    Get all fuzzy matches for a name, sorted by match score.
    
    Args:
        name (str): Name to match
        choices (List[str]): List of possible matches
        scorer (callable): Function to use for scoring matches
        
    Returns:
        List[Tuple[str, int]]: List of (match, score) tuples, sorted by score
        
    Raises:
        FuzzyMatchError: If matching process fails
    """
    try:
        if not isinstance(name, str):
            raise ValueError("Name must be a string")
        if not isinstance(choices, list) or not all(isinstance(x, str) for x in choices):
            raise ValueError("Choices must be a list of strings")
            
        matches = scorer(name, choices)
        return sorted(matches, key=lambda x: x[1], reverse=True)
        
    except Exception as e:
        logging.error(f"Error in sorted fuzzy matching for '{name}': {str(e)}")
        raise FuzzyMatchError(f"Failed to perform sorted fuzzy match: {str(e)}")
    
def map_name_match(df: pd.DataFrame, fbref_df: pd.DataFrame) -> pd.DataFrame:
    
    try:
        matched_indices = []
        
        if "fuzzy_match" not in df.columns:
            raise DataProcessingError("DataFrame does not contain column fuzzy_match")
        
        # Looping over matched entries
        for idx, row in df[~df["fuzzy_match"].isna()].iterrows():
            match = row["fuzzy_match"]
            
            matching_ids = fbref_df[fbref_df["name"] == match]["id"]
            if not matching_ids.empty:
                df.at[idx, "id_fbref"] = matching_ids.iloc[0]
                matched_indices.append(idx)

        df.update(df.loc[matched_indices])
        return df
    
    except Exception as e:
        raise DataProcessingError(f"Error matching names: {str(e)}")

def load_dfs(fbref_id_path: str, fpl_id_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and preprocess FBRef and FPL player data.
    
    Args:
        fbref_id_path (str): Path to FBRef IDs CSV file
        fpl_id_path (str): Path to FPL IDs CSV file
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Preprocessed FBRef and FPL DataFrames
        
    Raises:
        DataProcessingError: If loading or preprocessing fails
    """
    try:
        # Load the files
        fbref_ids = pd.read_csv(fbref_id_path)
        fpl_ids = pd.read_csv(fpl_id_path)
        
        # Process FPL names
        fpl_ids['full_name_abbr'] = (fpl_ids['first_name'].str.split(" ").str[0] + 
                                    " " + 
                                    fpl_ids["second_name"].str.split(" ").str[-1])
        fpl_ids['full_name_full'] = fpl_ids["first_name"] + " " + fpl_ids["second_name"]
        
        # Process FBRef names
        fbref_ids[['first_name', 'second_name']] = fbref_ids['name'].str.split(' ', n=1, expand=True)
        
        return fbref_ids, fpl_ids
        
    except Exception as e:
        raise DataProcessingError(f"Failed to load and process DataFrames: {str(e)}")

def save_data(
    matched_df: pd.DataFrame,
    missing_players: List[str],
    season: str ) -> None:
    """
    Save processed data to CSV and JSON files.
    
    Args:
        matched_df: DataFrame containing matched player IDs
        missing_players: List of player names without matches
        season: String containing season
        
    Raises:
        DataProcessingError: If saving files fails
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = f"/usr/local/airflow/include/data/results/{season}"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Save matched players to CSV
        matched_csv_path = Path(output_dir) / "player_compiled_ids.csv"
        matched_df.to_csv(matched_csv_path, index=False)
        logging.info(f"Saved matched players to {matched_csv_path}")
        
        # Save missing players to JSON
        missing_json_path = Path(output_dir) / "missing_fbref_ids.json"
        
        with open(missing_json_path, "w") as file_out:
            json.dump(missing_players, file_out)
        logging.info(f"Saved missing players to {missing_json_path}")
            
    except Exception as e:
        raise DataProcessingError(f"Failed to save data: {str(e)}")

def process_player_matches(
    unmatched: pd.DataFrame,
    matched: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    """
    Process and combine matched and unmatched player data.
    
    Args:
        unmatched: DataFrame containing unmatched players
        matched: DataFrame containing already matched players
        
    Returns:
        tuple containing:
            - DataFrame with all matched players
            - List of names of players still missing matches
    """
    try:
        # Extract successfully matched players from previously unmatched data
        recovered_matches = unmatched[~unmatched["id_fbref"].isna()].copy()
        recovered_matches.loc[:, "name"] = recovered_matches["fuzzy_match"]
        
        columns_to_keep = ["first_name", "second_name", "name", "id_fpl", "id_fbref"]
        recovered_matches = recovered_matches[columns_to_keep]
        
        matched = pd.concat([matched, recovered_matches], axis=0)
        
        # Remove any duplicate columns
        matched = matched.drop_duplicates("id_fpl", keep= "first")
        
        # Get list of still unmatched players
        missing_players = unmatched[unmatched["id_fbref"].isna()]["full_name_full"].tolist()
        fpl_cols = ["first_name", "second_name", "id_fpl"]
        all_players = pd.concat([matched, unmatched], axis= 0)[fpl_cols].drop_duplicates().sort_values("id_fpl")
        
        merged_df = all_players.merge(
            matched,
            left_on= fpl_cols,
            right_on= fpl_cols,
            how="left").rename(columns={
                            "name": "name_fbref",
                            "first_name": "first_name_fpl",
                            "second_name": "second_name_fpl"
                            })
        
        return merged_df, missing_players
        
    except Exception as e:
        raise DataProcessingError(f"Failed to process player matches: {str(e)}")

def sift_names(main_df: pd.DataFrame,
               fbref_df: pd.DataFrame,
               level: str,
               name: str) -> pd.DataFrame:
    """
    Interactively match player names between two dataframes using fuzzy matching.
    
    Args:
        main_df (pd.DataFrame): Main dataframe containing player names to match
        fbref_df (pd.DataFrame): Reference dataframe with FBRef player data
        level (str): Matching strictness ('strict' or 'loose')
        name (str): Which name to match on ('first' or 'last')
    
    Returns:
        pd.DataFrame: Updated dataframe with matched names and IDs
    
    Raises:
        NameSiftingError: If there are issues with input validation or data processing
        ValueError: If invalid parameters are provided
    """
    # Input Validation
    if not isinstance(main_df, pd.DataFrame) or not isinstance(fbref_df, pd.DataFrame):
        raise ValueError("Both df and fbref_df must be pandas DataFrames")
    if level not in ['strict', 'loose']:
        raise ValueError("level must be either 'strict' or 'loose'")
    if name not in ['first', 'last']:
        raise ValueError("name must be either 'first' or 'last'")
    
    # Required columns validation
    required_cols = {'first_name', 'second_name', 'fuzzy_match'}
    fbref_required_cols = {'name', 'id'}
    
    if not all(col in main_df.columns for col in required_cols):
        raise ValueError(f"Main DataFrame missing required columns: {required_cols}")
    
    if not all(col in fbref_df.columns for col in fbref_required_cols):
        raise ValueError(f"FBRef DataFrame missing required columns: {fbref_required_cols}")
    
    df = main_df.copy()
    try:
        matched_indices = []
        missing_df = df[df["fuzzy_match"].isna()]
        
        for i, (idx, row) in enumerate(missing_df.iterrows(), 1):
            try:
                fname = row['first_name']
                lname = row['second_name']
                
                logging.debug(f"Processing: {fname} {lname}")
                
                if name == "first":
                    filter_name = remove_special_letters(fname)
                else: # name == "last"
                    filter_name = remove_special_letters(lname.split(" ")[-1])
                
                # finding potential matches
                fbref_names = fbref_df[fbref_df["name"].str.contains(
                    filter_name,case=False, na=False)]["name"].tolist()
                found_names = df[~df["fuzzy_match"].isna()]["fuzzy_match"].tolist()
                need_matching_names = [name for name in fbref_names if name not in found_names]

                if need_matching_names:
                    # sifting strictness
                    if level == "strict":
                        related_names = sorted_fuzzy_match(fname if name == "first" else lname, need_matching_names)
                        related_names = [name for name, score in related_names]
                    else: # level == "loose"
                        related_names = need_matching_names

                    print(f"\n({i}/{missing_df.shape[0]}) Matching for: {fname} {lname}")
                    
                    for j, (related_name) in enumerate(related_names, 1):
                        print(f"{j}. {related_name}")
                    
                    # print("0. None of the above")
                    
                     # Handle user input
                    while True:
                        try:
                            choice = input("Enter the number of the correct match (Enter for none): ")
                            
                            if choice == "":
                                logging.info("No match selected.")
                                break
                                                            
                            choice = int(choice)
                            
                            if choice == 0:
                                logging.info("No match selected.")
                                break
                            
                            elif 1 <= choice <= len(related_names):
                                selected_name = related_names[choice - 1]
                                matching_id = fbref_df[fbref_df["name"] == selected_name]["id"].iloc[0]
                                
                                # Update dataframe
                                df.at[idx, "fuzzy_match"] = selected_name
                                df.at[idx, "id_fbref"] = matching_id
                                matched_indices.append(idx)
                                logging.info(f"Selected: {selected_name} (ID: {matching_id})")
                                break
                            
                            else:
                                print("Invalid choice. Please enter a number between 0 and", 
                                      len(related_names))
                        except ValueError:
                            print("Invalid input. Please enter a number.")
                    
                else:
                    # logging.info(f"No matches found for {fname} {lname}")
                    pass

            except Exception as e:
                logging.error(f"Error processing row {idx}: {str(e)}")
                continue
            
        df.update(df.loc[matched_indices])
        return df
    
    except Exception as e:
        raise DataProcessingError(f"Error sifting through names: {str(e)}")

def manual_sift(main_df: pd.DataFrame) -> pd.DataFrame:
    """
    Manual sifting function for unmatched players as a final resort.
    Allows manual input of FBRef IDs for remaining unmatched players.
    
    Args:
        main_df (pd.DataFrame): DataFrame containing player information with columns:
                          'id_fbref' and 'full_name_full'
    
    Returns:
        pd.DataFrame: Updated DataFrame with manually matched FBRef IDs
    
    Raises:
        ValueError: If input validation fails
        DataProcessingError: If there are issues during the sifting process
    """
    
    if not isinstance(main_df, pd.DataFrame):
        raise ValueError("df must be a pd.DataFrame")
    
    required_rows = set(["id_fbref", "full_name_full"])
    if not all([col in main_df.columns for col in required_rows]):
        raise ValueError(f"df is missing required columns: {required_rows}")
    
    df = main_df.copy()
    unmatched_count = df["id_fbref"].isna().sum()
    
    if unmatched_count == 0:
        logging.info("No unmatched players found.")
        return df
    
    logging.info(f"Starting final resort sift for {unmatched_count} unmatched players...\n")
    
    try:
        na_leng = len(df[df["id_fbref"].isna()])
        iteration = 0
        for idx, row in df[df["id_fbref"].isna()].iterrows():
            iteration += 1
            name = row["full_name_full"]
            response = input(f"({iteration}/{na_leng}) Enter FBRef link for {name} (press Enter to skip): ")
            # pyperclip.copy(name)
            
            if response == "":
                row["id_fbref"] = np.NaN
                logging.debug(f"Skipped player: {name}")
            
            else:
                url = str(response)
                path_content_list = urlparse(url).path.split("/")
                if len(path_content_list) == 5:
                    id = path_content_list[3]
                    name = path_content_list[4].replace("-", " ")
                else:
                    id = np.NaN
                
                df.at[idx, "id_fbref"] = id
                df.at[idx, "name"] = name
        
        return df
    
    except Exception as e:
        raise DataProcessingError(f"Error sifting through names: {str(e)}")
    
def main():
    try:
        # Define file paths
        fbref_path = "data/2024-25/fbref_ids.csv"
        fpl_path = 'data/2024-25/player_idlist.csv'
        
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
        
        # Split into matched and unmatched players
        unmatched = merged[merged["id_fbref"].isna()].copy()
        matched = merged[~merged["id_fbref"].isna()].drop(
            columns=[col for col in merged.columns
                    if col not in ["first_name", "second_name", "name", "id_fpl", "id_fbref"]]
        )
        
        # Perform fuzzy matching
        logging.info("Performing fuzzy matching...")
        threshold = 95
        unmatched.loc[:, 'fuzzy_match'] = unmatched['full_name_abbr'].apply(
            lambda name: fuzzy_match(name, fbref_ids['name'].tolist(), threshold)
        )
        unmatched = map_name_match(unmatched, fbref_ids)
        
        # # Sifting names
        # logging.info("Strict Last Sifting...")
        # unmatched = sift_names(unmatched, fbref_ids, "strict", "last")
        # time.sleep(5)
        
        # logging.info("Loose First Sifting...")
        # unmatched = sift_names(unmatched, fbref_ids, "loose", "first")
        # time.sleep(5)
        
        # logging.info("Manual Sifting...")
        # unmatched = manual_sift(unmatched)
        # print(unmatched.loc[unmatched["fuzzy_match"].isna()].head(5))
        
        # Process final results
        # logging.info("Processing results...")
        final_matched, missing_players = process_player_matches(unmatched, matched)
        
        # # Save results
        # logging.info("Saving results...")
        # save_data(final_matched, missing_players, "2024-25")
        
        # logging.info("Processing completed successfully")
        # return 0
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())