
def source_column_matching_prompt() -> str:
    # Prompt template for column matching
    prompt_template = """
    <s> [INST]
    The following are standard column names: {standard_columns}.
    I will provide you with the column names and the first 5 rows of a table. 
    Your task is to match each of the standard column names to the closest column name from the table.
    If a match is not clear, use your best judgment.

    Standard Columns: {standard_columns}
    Table Columns: {table_columns}
    First 5 Rows: {table_sample}

    Provide a dictionary in the format:
    {{
        "Standard Column Name": "Table Column Name"
    }}

    [INST] Make sure that:
    1. All property names are enclosed in double quotes.
    2. All string values are also enclosed in double quotes.
    3. No trailing commas are present in arrays or objects.
    4. The final JSON maintains the same data as provided in the source.
    5. Any leading or trailing text is removed before processing.
    6. If the output is JSON with a dictionary, return it as-is without any additional text.
    7. If the output is JSON with a list, return it as-is without any additional text.
    8. If the input is an empty list, return an empty list (`[]`).
    9. If the input is an empty dictionary, return an empty dictionary (`{{}}`).
    10. Do not include any disclaimers, explanations, or additional information.
    
    Just output the JSON dictionary or list as described.

    [/INST] 
    [/INST] <s>
    """

    return prompt_template



def row_difference_analysis_prompt(modification_details: dict) -> str:
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


def get_clean_json_prompt(rough_json):
    clean_json_prompt = f"""
        <s>
        [INST]
        You are a JSON cleaning assistant. Please clean the following rough JSON data. Make sure that:

        1. All property names are enclosed in double quotes.
        2. All string values are also enclosed in double quotes.
        3. No trailing commas are present in arrays or objects.
        4. The final JSON maintains the same data as provided in the source.
        5. Any leading or trailing text should be removed before processing.
        6. You can handle extract JSON with dict or JSON with list from text
        7. If the input is empty list return it list if the input is empty dict return it empty dict    
        [/INST]

        Input:
        {rough_json}
        Return the json from the Input in the correct format
        </s>
        """
    return clean_json_prompt