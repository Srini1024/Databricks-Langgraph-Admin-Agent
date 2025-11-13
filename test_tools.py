# Databricks notebook source
import json
import logging
import os

import requests
from dotenv import load_dotenv
from langchain_core.tools import tool
from typing import Optional, Dict, Any, Union

logger = logging.getLogger(__name__)

DATABRICKS_BASE_URL = "YOUR DATABRICKS BASE URL"
DATABRICKS_TOKEN = "YOUR DATABRICKS PAT TOKEN"

def _get_auth_headers() -> dict[str, str]:
        """Helper to generate standard authorization headers."""
        return {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }


def get_job_details(
    job_id: int, 
    page_token: Optional[str] = None
) -> str:
    """
    Retrieves the full configuration and metadata for a single job definition via the Databricks REST API.
    
    The 'job_id' parameter is required.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.2/jobs/get"
    
    try:
        request_data = {
            "job_id": job_id,
        }
        
        if page_token:
            request_data["page_token"] = page_token

       
        response = requests.get(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()
        
        result = response.json()
        return json.dumps(result)

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "get_job"})
get_job_details(1031228529911077)
