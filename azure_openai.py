from openai import AzureOpenAI
from config import api_key, api_base, api_version, deployment_name

class LLM_Azure:
    def __init__(self):
        """Initialize with Azure OpenAI credentials"""
        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=api_base
        )
        self.deployment_name = deployment_name
        
    def get_completion(self, prompt: str) -> str:
        """Get completion from Azure OpenAI"""
        response = self.client.chat.completions.create(
            model=self.deployment_name,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        return response.choices[0].message.content