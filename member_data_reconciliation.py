import hashlib
import pandas as pd
from difflib import SequenceMatcher
from typing import List, Tuple
from openai import AzureOpenAI
from prompts import row_difference_analysis_prompt
from azure_openai import LLM_Azure

def generate_md5_hash(row):
    row_data = ','.join(str(val) for val in row)
    return hashlib.md5(row_data.encode()).hexdigest()

def generate_hashes_from_csv(file_path):
    df = pd.read_csv(file_path)
    hash_dict = {}
    for idx, row in df.iterrows():
        row_hash = generate_md5_hash(row)
        hash_dict[row_hash] = row['Member ID']
    return hash_dict, df

def compare_csv_integrity(file1, file2):
    file1_hash_dict, df1 = generate_hashes_from_csv(file1)
    file2_hash_dict, df2 = generate_hashes_from_csv(file2)

    missing_rows = []
    altered_rows = []

    for row_hash, member_id in file1_hash_dict.items():
        if row_hash not in file2_hash_dict:
            if member_id not in df2['Member ID'].values:
                missing_rows.append(member_id)
            else:
                row1 = df1[df1['Member ID'] == member_id].iloc[0]
                row2 = df2[df2['Member ID'] == member_id].iloc[0]
                altered_columns = []
                for col in df1.columns:
                    if row1[col] != row2[col]:
                        altered_columns.append(col)
                if altered_columns:
                    altered_rows.append((member_id, altered_columns))

    return missing_rows, altered_rows

def calculate_string_similarity(str1: str, str2: str) -> float:
    """Calculate similarity ratio between two strings"""
    return SequenceMatcher(None, str(str1), str(str2)).ratio()

class CSVIntegrityAgent:
    def __init__(self,):
        """Initialize with Azure OpenAI credentials"""
        self.llm = LLM_Azure()
        
    def analyze_modification(self, df1: pd.DataFrame, df2: pd.DataFrame, member_id: int, column: str) -> dict:
        """Analyze the specific modification for a given member ID and column"""
        original_value = df1[df1['Member ID'] == member_id][column].iloc[0]
        modified_value = df2[df2['Member ID'] == member_id][column].iloc[0]
        
        similarity = calculate_string_similarity(original_value, modified_value)
        
        return {
            'member_id': member_id,
            'column': column,
            'original_value': original_value,
            'modified_value': modified_value,
            'similarity_score': similarity
        }

    def create_analysis_prompt(self, modification_details: dict) -> str:
        """Create a prompt for the LLM to analyze the modification"""
        return f"""Analyze the following modification in a CSV record:
        Member ID: {modification_details['member_id']}
        Column: {modification_details['column']}
        Original Value: {modification_details['original_value']}
        Modified Value: {modification_details['modified_value']}
        Similarity Score: {modification_details['similarity_score']}

        Based on these details, determine if this modification appears to be:
        1. A typo correction
        2. A legitimate data update
        3. A potentially concerning change

        Consider:
        - The similarity between the original and modified values
        - The type of field being modified
        - Common patterns in data errors

        If the difference looks like spelling or typing error, call it a typo.
        If it looks like an update over a previous data, call it a legitimate update.
        If the difference is something too random, then it might be a concering change.

        IMPORTANT: Respond with your analysis and at the end, on a new line, write exactly one of these three categories:
        CATEGORY: TYPO
        or
        CATEGORY: UPDATE
        or
        CATEGORY: CONCERNING

        Provide your analysis and category without reasoning. Please return only the statement 
        indicating what kind of modification it appears to take be."""

    def get_modification_category(self, analysis_text: str) -> str:
        """Extract the category from the analysis text"""
        if "CATEGORY: TYPO" in analysis_text:
            return "TYPO"
        elif "CATEGORY: UPDATE" in analysis_text:
            return "UPDATE"
        elif "CATEGORY: CONCERNING" in analysis_text:
            return "CONCERNING"
        return "CONCERNING"  # Default to concerning if category is unclear

    def analyze_and_update_changes(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                 missing_rows: List[int], 
                                 altered_rows: List[Tuple[int, List[str]]], 
                                 output_file: str = None) -> tuple[str, pd.DataFrame]:
        """Analyze changes and update df2 based on analysis results"""
        analysis_results = []
        df2_updated = df2.copy()
        updates_made = []
        
        # Analyze missing rows
        if missing_rows:
            analysis_results.append(f"\nMissing Records Analysis:")
            analysis_results.append(f"Found {len(missing_rows)} missing records: {missing_rows}")
            # Add missing rows from df1 to df2
            for member_id in missing_rows:
                missing_row = df1[df1['Member ID'] == member_id]
                if not missing_row.empty:
                    df2_updated = pd.concat([df2_updated, missing_row], ignore_index=True)
                    updates_made.append(f"Added missing record for Member ID: {member_id}")
        
        # Analyze modified rows
        if altered_rows:
            analysis_results.append(f"\nModified Records Analysis:")
            for member_id, columns in altered_rows:
                for column in columns:
                    mod_details = self.analyze_modification(df1, df2, member_id, column)
                    prompt = row_difference_analysis_prompt(mod_details)
                    analysis = self.llm.get_completion(prompt)
                    category = self.get_modification_category(analysis)
                    
                    analysis_results.append(f"\nMember ID: {member_id} - {column}")
                    analysis_results.append(f"Original: {mod_details['original_value']}")
                    analysis_results.append(f"Modified: {mod_details['modified_value']}")
                    analysis_results.append(f"Similarity Score: {mod_details['similarity_score']:.2f}")
                    analysis_results.append(f"Analysis: {analysis}")
                    analysis_results.append(f"Category: {category}")
                    
                    # Update df2 if the change is a typo or legitimate update
                    if category in ["TYPO", "UPDATE"]:
                        df2_updated.loc[df2_updated['Member ID'] == member_id, column] = \
                            df1.loc[df1['Member ID'] == member_id, column].iloc[0]
                        updates_made.append(f"Updated {column} for Member ID: {member_id} (Category: {category})")
        
        if updates_made:
            analysis_results.append("\nUpdates Made:")
            analysis_results.extend(updates_made)
        
        # Save updated DataFrame if output_file is provided
        if output_file:
            df2_updated.to_csv(output_file, index=False)
            analysis_results.append(f"\nUpdated data saved to: {output_file}")
        
        return "\n".join(analysis_results), df2_updated


def main():
    agent = CSVIntegrityAgent()

    # Read your CSV files
    df1 = pd.read_csv("dataIntegrityTest1.csv")
    df2 = pd.read_csv("dataIntegrityTest2.csv")

    # Get the comparison results
    missing_rows, altered_rows = compare_csv_integrity("dataIntegrityTest1.csv", "dataIntegrityTest2.csv")

    # Analyze the changes and get updated DataFrame
    analysis_result, updated_df = agent.analyze_and_update_changes(
        df1, df2, missing_rows, altered_rows, 
        output_file="mergedFile.csv"
    )
    print(analysis_result)

if __name__=="__main__":
    main()