#!/usr/bin/env python3
"""
safe_graphql.py - Safe GraphQL query execution utilities
This module provides functions for safely executing GraphQL queries with error handling.
"""

import json
import logging
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
LOGGER = logging.getLogger()

def safe_graphql_query(manager, query: str, variables: Dict = None, error_message: str = "Query failed") -> Dict:
    """
    Safely execute a GraphQL query with error handling
    
    Args:
        manager: MonitorManager instance
        query: GraphQL query string
        variables: Optional variables dictionary
        error_message: Custom error message
        
    Returns:
        Query result or empty dictionary
    """
    try:
        # Log the query for debugging
        LOGGER.info(f"Executing query: {query}")
        if variables:
            LOGGER.info(f"Variables: {json.dumps(variables, indent=2)}")
        
        # Execute the query
        result = manager.mc_client.execute_query(query, variables or {})
        
        # Log the result for debugging
        if hasattr(result, "_data"):
            result_dict = result._data
        else:
            result_dict = result
            
        # Convert to regular dict for logging
        if hasattr(result_dict, "__dict__"):
            result_dict = result_dict.__dict__
        
        LOGGER.info(f"Query result: {json.dumps(result_dict, indent=2)}")
        
        return result
    except Exception as e:
        LOGGER.error(f"{error_message}: {str(e)}")
        return {}