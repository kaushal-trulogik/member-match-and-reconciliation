import json
import re
from functools import wraps
from prompts import get_clean_json_prompt
from azure_openai import LLM_Azure

llm = LLM_Azure()


def is_json(response_text):
    try:
        json.loads(response_text)
        return True
    except (ValueError, json.JSONDecodeError):
        return False


def convert_json(json_data):
    if "{" in json_data and "}" in json_data:
        start = json_data.find("{")
        end = json_data.rfind("}") + 1
        json_data = json_data[start:end]

    elif "[" in json_data and "]" in json_data:
        start = json_data.find("[")
        end = json_data.rfind("]") + 1
        json_data = json_data[start:end]
    json_data.replace("'", '"')
    # json_data = re.sub(r',\s*([}\]])', r'\1', json_data) # TODO uncomment this line if required to Remove trailing commas before closing braces and brackets
    if is_json(json_data):
        return json.loads(json_data)
    return None


def validate_json(max_attempts=3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                content = func(*args, **kwargs)

                final_data = convert_json(content)
                if final_data is not None:
                    return final_data
                else:
                    content = llm.get_completion(get_clean_json_prompt(content))
                    final_data = convert_json(content)
                    if final_data is not None:
                        return final_data

                print("Content is not a valid JSON. Attempting again...")
                attempts += 1
            print("func.__name__", func.__name__)
            print("content", content)
            raise Exception(
                f"Failed to retrieve valid JSON after {max_attempts} attempts."
            )

        return wrapper

    return decorator


# Example usage
@validate_json()
def get_response(prompt):
    # Simulate fetching data that may or may not be valid JSON
    # Replace this with your actual logic
    return '{"key": "value}'  # Example valid JSON response



if __name__=="__main__":
    try:
        result = get_response("some prompt")
        print(result)
    except Exception as e:
        print(e)