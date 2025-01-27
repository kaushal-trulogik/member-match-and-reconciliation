from azure_openai import LLM_Azure
from typing import List, Dict
from sqlalchemy import create_engine
from prompts import source_column_matching_prompt
import pandas as pd
import json
from config import db_config
from json_validator import validate_json

class ColumnMatchAgent:
    """
    A class for handling column matching tasks using LLM and PostgreSQL.
    """

    def __init__(self, llm: LLM_Azure):
        """
        Initialize with database configuration and LLM instance.
        """
        self.llm = llm
        self.prompt_template = source_column_matching_prompt()

    def connect_db(self, db_config: Dict[str, str],):
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
    
    def get_table_data(self, table_name: str) -> Dict:
        """
        Retrieve column names and first 5 rows of data from the specified table.
        """
        query = f"SELECT * FROM {table_name} LIMIT 5;"
        df = pd.read_sql_query(query, self.engine)
        return {"columns": list(df.columns), "sample": df.head(5).to_dict(orient="records")}
    
    def prepare_prompt(self, standard_columns, table_data: Dict) -> str:
        """
        Prepare the prompt for the LLM.
        """
        return self.prompt_template.format(
            standard_columns=", ".join(standard_columns),
            table_columns=", ".join(table_data["columns"]),
            table_sample=table_data["sample"]
        )
    
    @validate_json()
    def process_table(self, standard_columns, table_name: str) -> Dict:
        """
        Process a table to get column mappings using LLM.
        """
        table_data = self.get_table_data(table_name)
        prompt = self.prepare_prompt(standard_columns, table_data)
        response = self.llm.get_completion(prompt)
        response = self.llm.get_completion(prompt)
        # print(response)
        return response
    
    
    def match_columns_for_tables(self, standard_columns, tables: List[str]) -> Dict:
        """
        Match columns for all specified tables.
        """
        final_mappings = {}
        for table in tables:
            print(f"Processing table: {table}")
            final_mappings[table] = self.process_table(standard_columns, table)
            print(f"Mapping for {table}: {final_mappings[table]}")
        return final_mappings


# Example Usage
if __name__ == "__main__":
    # Initialize LLM
    llm = LLM_Azure()

    # Standard Columns
    standard_columns = ["Member ID", "First Name", "Last Name", "DOB", "Address", "City", "State", "Zip"]

    # Initialize Agent
    agent = ColumnMatchAgent(llm)
    agent.connect_db(db_config)

    # Tables to process
    tables = [
        "member_integrity_check_1",
        # "member_integrity_check_2",
        # "member_integrity_check_3",
        "member_integrity_check_4",
        ]

    # Get mappings
    mappings = agent.match_columns_for_tables(standard_columns, tables)
    print(mappings)
    print("\nFinal Mappings:")
    print(json.dumps(mappings, indent=4))