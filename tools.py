import json
import os
import requests
from typing import Dict, Any, Optional, Union
from langchain_core.tools import tool

DATABRICKS_BASE_URL = "YOUR DATABRICKS BASE URL"
DATABRICKS_TOKEN = "YOUR DATABRICKS PAT TOKEN"

def _get_auth_headers() -> dict[str, str]:
        """Helper to generate standard authorization headers."""
        return {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }


"""Service Principal/SP tools""" 

@tool
def list_service_principal(
    filter: Optional[str] = None,
    count: Optional[int] = None,
    sortBy: Optional[str] = None,
    sortOrder: Optional[str] = None,
    startIndex: Optional[int] = None,
    attributes: Optional[str] = None,
    excludedAttributes: Optional[str] = None,
) -> str:
    """
    Lists all the service principal/SP via the Databricks REST API.
    
    Args:
        filter: SCIM filter expression (e.g., 'displayName co "sp"').
        count: Desired number of results per page (max 100).
        sortBy: Attribute to sort results by.
        sortOrder: The order to sort ('ascending' or 'descending').
        startIndex: Index of the first result (1-based).
        attributes: Comma-separated list of attributes to include in the response.
        excludedAttributes: Comma-separated list of attributes to exclude.
        
    Returns:
        A JSON string containing the SCIM API response (list of SPs) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/preview/scim/v2/ServicePrincipals"
    
    try:
        request_data = {}
        
        if filter is not None:
           request_data["filter"] = filter
        if count is not None:
           request_data["count"] = count
        if sortBy is not None:
           request_data["sortBy"] = sortBy
        if sortOrder is not None:
           request_data["sortOrder"] = sortOrder
        if startIndex is not None:
           request_data["startIndex"] = startIndex
        if attributes is not None:
           request_data["attributes"] = attributes
        if excludedAttributes is not None:
           request_data["excludedAttributes"] = excludedAttributes
       
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
        return json.dumps({"error": error_message, "tool": "list_service_principal"})

@tool
def create_service_principal(
    display_name: str,
    active: bool = True,
    application_id: Optional[str] = None,
) -> str:
    """
    Create a service principal/SP via the Databricks REST API.
    
    Args:
        display_name: The user-friendly name for the Service Principal (required).
        active: Whether the SP should be active (defaults to True).
        application_id: Optional UUID/Client ID if you are creating an SP to represent 
                        an existing external identity (like an Azure AD SP).
        
    Returns:
        A JSON string containing the SCIM API response (list of SPs) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/preview/scim/v2/ServicePrincipals"
    
    try:
        request_data = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "displayName": display_name,
            "active": active,
        }
        
        if application_id is not None:
             request_data["applicationId"] = application_id
       
        response = requests.post(
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
        return json.dumps({"error": error_message, "tool": "create_service_principal"})

@tool
def get_service_principal_details(
    id: int,
) -> str:
    """
   Gets a service principal/SP via the Databricks REST API.
    
    Args:
        id: The ID of the SP to get.
        
    Returns:
        A JSON string containing the SCIM API response (list of SPs) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/preview/scim/v2/ServicePrincipals/{id}"
    
    try:
        request_data = {
            "id": id
        }
        
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
        return json.dumps({"error": error_message, "tool": "get_service_principal_details"})

@tool
def delete_service_principal(
    id: int,
) -> str:
    """
    Deletes a service principal/SP via the Databricks REST API.
    
    Args:
       id: The ID of the SP to delete.
        
    Returns:
        A JSON string containing the SCIM API response (list of SPs) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/preview/scim/v2/ServicePrincipals/{id}"
    
    try:
        
        response = requests.delete(
            api_endpoint,
            headers=_get_auth_headers(),
            timeout=30.0
        )
        response.raise_for_status()
        success_message = f"Service Principal with ID {id} deleted successfully."
        return json.dumps({"status": "Success", "message": success_message})
    

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "delete_service_principal"})



"""Scopes & Secrets tools"""

@tool
def list_of_scopes() -> str:
    """Gets the list of secret scopes via the Databricks REST API."""

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/scopes/list"
    
    try:

        response = requests.get(
            api_endpoint,
            headers=_get_auth_headers(),
            timeout=30.0
        )
        response.raise_for_status()  
        
        result = response.json().get("scopes", [])
        return json.dumps(result)

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "list_of_scopes"})

@tool
def create_scope(
    scope: str, 
    scope_backend_type: str = "DATABRICKS",
    initial_manage_principal: Optional[str] = None,
) -> str:
    """
    Creates a new scope via the Databricks REST API.
    
    Args:
        scope: The name of the secret scope to create.
        scope_backend_type: The type of secret scope. Can be "DATABRICKS" or "AZURE_KEYVAULT".
                            Defaults to "DATABRICKS".
        initial_manage_principal: The principal that is initially granted MANAGE permission.
                                  Defaults to none.
                     
    Returns:
        A JSON string containing the API response (empty on success) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/scopes/create"
    
    try:
        request_data = {
            "scope": scope,
            "scope_backend_type": scope_backend_type,
        }
        
        if initial_manage_principal is not None:
             request_data["initial_manage_principal"] = initial_manage_principal

       
        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()
        
        success_message = f"Scope {scope} created successfully."
        return json.dumps({"status": "Success", "message": success_message})
    

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "create_scope"})

@tool
def add_secret(
    scope: str, 
    key: str,
    string_value: Optional[str],
    bytes_value: Optional[str],
) -> str:
    """
    Inserts a secret under the provided scope with the given name. If a secret already exists with the same name, this command overwrites the existing secret's value via the Databricks REST API.
    
    Args:
        scope: The name of the scope.
        key: The name of the secret to create.
        string_value : If specified, note that the value will be stored in UTF-8 (MB4) form.
        bytes_value: If specified, value will be stored as bytes.
        
    Returns:
        A JSON string containing the API response (empty on success) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/put"
    
    try:
        request_data = {
            "scope": scope,
            "key": key,
        }
        
        if string_value:
            request_data["string_value"] = string_value
        if bytes_value:
            request_data["bytes_value"] = bytes_value

       
        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()
        success_message = f"Secret with key {key} stored successfully."
        return json.dumps({"status": "Success", "message": success_message})
    

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "create_secret"})
 
@tool
def delete_secret(
    scope: str, 
    key: str,
) -> str:
    """
    Deletes a secret under the provided scope with the given name via the Databricks REST API.
    
    Args:
        scope: The name of the scope.
        key: The name of the secret to delete.
        
    Returns:
        A JSON string containing the API response (empty on success) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/delete"
    
    try:
        request_data = {
            "scope": scope,
            "key": key,
        }
        
       
        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()
        success_message = f"Secret with key {key} deleted successfully."
        return json.dumps({"status": "Success", "message": success_message})
    

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "delete_secret"})

@tool
def delete_scope(
    scope: str,
) -> str:
    """
   Deletes a scope via the Databricks REST API.
    
    Args:
        scope: The name of the secret scope to create.
                     
    Returns:
        A JSON string containing the API response (empty on success) or an error message.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/scopes/delete"
    
    try:
        request_data = {
            "scope": scope,
        }
        

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()
        
        success_message = f"Scope {scope} deleted successfully."
        return json.dumps({"status": "Success", "message": success_message})
    
    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "delete_scope"})

@tool
def get_secret(
    scope: str, 
    key: str,
) -> str:
    """
    Gets the secret under the provided scope with the given name via the Databricks REST API.
    
    Args:
        scope: The name of the scope.
        key: The name of the secret.
        
    Returns:
        A JSON string containing the API response (empty on success) or an error message.
    """
    
    try:
        secret_value = dbutils.secrets.get(scope=scope, key=key)
    
        return json.dumps({"key": key, "value": secret_value, "source": "dbutils"})
        

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "get_secret"})

@tool 
def create_acl_scopes(
    scope: str,
    permission: str,
    principal: str,
) -> str:
    """Creates or updates permission to the service principal to scopes via the Databricks REST API."""

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/acls/put"
    
    try:
        request_data = {
            "scope": scope,
            "principal": principal,
            "permission": permission
        }

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()  
        
        success_message = f"Gave {permission} permission to {principal} on {scope}."
        return json.dumps({"status": "Success", "message": success_message})


    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "create_acl_scopes"})

@tool
def list_acl_scopes(
    scope: str,
) -> str:
    """Lists the ACLs set on the given scope via the Databricks REST API."""

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/acls/list"
    
    try:
        request_data = {
            "scope": scope
        }

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
        return json.dumps({"error": error_message, "tool": "list_acl_scopes"})

@tool
def delete_acl_scopes(
    scope: str,
    principal: str,
) -> str:
    """Deletes the existing permission to the service principal to scopes via the Databricks REST API."""

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.0/secrets/acls/delete"
    
    try:
        request_data = {
            "scope": scope,
            "principal": principal,
        }

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()  
        
        success_message = f"Deleted permissions for {principal} on {scope}."
        return json.dumps({"status": "Success", "message": success_message})


    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "delete_acl_scopes"})

"""Cluster tools"""

@tool
def list_clusters() -> str: 
    """Gets the list of clusters via the Databricks REST API."""

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.1/clusters/list"
    
    try:

        response = requests.get(
            api_endpoint,
            headers=_get_auth_headers(),
            timeout=30.0
        )
        response.raise_for_status()  
        result = response.json().get("clusters", [])
        return json.dumps(result)

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "list_clusters"})
    
@tool
def terminate_clusters(cluster_id: str) -> str:
    """Terminates a cluster via the Databricks REST API.
       The 'cluster_id' parameter is required.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.1/clusters/delete"
    
    try:
        request_data = {
        "cluster_id": cluster_id
        }

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()  
        
        success_message = f"Cluster {cluster_id} terminated successfully."
        return json.dumps({"status": "Success", "message": success_message})

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "terminate_clusters"})

@tool
def start_cluster(cluster_id: str) -> str:
    """Starts a cluster via the Databricks REST API.
       The 'cluster_id' parameter is required.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.1/clusters/start"
    
    try:
        request_data = {
        "cluster_id": cluster_id
        }

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()  
        
        success_message = f"Cluster {cluster_id} started successfully."
        return json.dumps({"status": "Success", "message": success_message})

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "start_cluster"})
    
@tool
def get_cluster_info(cluster_id: str) -> str:
    """Gets the cluster information via the Databricks REST API.
       The 'cluster_id' parameter is required.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.1/clusters/get"
    
    try:
        request_data = {
        "cluster_id": cluster_id
        }

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
        return json.dumps({"error": error_message, "tool": "get_cluster_info"})

@tool 
def restart_cluster(cluster_id: str) -> str:
    """Restarts a cluster via the Databricks REST API.
       The 'cluster_id' parameter is required.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.1/clusters/restart"
    
    try:
        request_data = {
        "cluster_id": cluster_id
        }

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()  
        success_message = f"Cluster {cluster_id} restarted successfully."
        return json.dumps({"status": "Success", "message": success_message})

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "restart_cluster"})


"""Job tools""" 
@tool 
def start_job(job_id: int, job_parameters: Optional[str] = None) -> str:
    """Triggers a job run via the Databricks REST API.
       The 'job_id' parameter is required.
       The 'job_parameters' should be a JSON string if provided.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.2/jobs/run-now"
    
    try:
        request_data = {
        "job_id": job_id
        }

        if job_parameters:
            request_data["json_parameters"] = json.loads(job_parameters)

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status()  
        
        success_message = f"Job: {job_id} started successfully."
        return json.dumps({"status": "Success", "message": success_message})

    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "start_job"})

@tool
def list_jobs(
    limit: int = 20, 
    expand_tasks: bool = False, 
    name: Optional[str] = None, 
    page_token: Optional[str] = None
) -> str:
    
    """List all jobs via the Databricks REST API."""


    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.2/jobs/list"
    
    try:

        request_data: Dict[str, Union[str, int, bool]] = {
        "limit": limit,
        "expand_tasks": expand_tasks,
        }

        if name:
            request_data["name"] = name
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
        return json.dumps({"error": error_message, "tool": "list_jobs"})

@tool
def cancel_job(job_id: int) -> str:
    """Cancel a job run via the Databricks REST API
       The 'job_id' parameter is required.
    """

    api_endpoint = f"{DATABRICKS_BASE_URL}/api/2.2/jobs/runs/cancel-all"
    
    try:

        request_data = {
            "job_id": job_id
        }

        response = requests.post(
            api_endpoint,
            headers=_get_auth_headers(),
            json=request_data,
            timeout=30.0
        )
        response.raise_for_status() 
        return f"Job: {job_id} cancelled succesfully"
    
    except Exception as e:
        error_message = f"Databricks API Error: {str(e)}"
        return json.dumps({"error": error_message, "tool": "list_jobs"})

@tool
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


    

