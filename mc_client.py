#!/usr/bin/env python3
"""
mc_client.py - Monte Carlo API Client
This module provides a wrapper for the Monte Carlo API client and utilities.
"""

import os
import sys
import json
import logging
import configparser
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
LOGGER = logging.getLogger()

# Check if additional dependencies are available, install if needed
try:
    from gql import gql, Client as GQLClient
    from gql.transport.requests import RequestsHTTPTransport
except ImportError:
    LOGGER.error("The gql package is required but not installed.")
    LOGGER.info("Install it using: pip install gql[requests]")
    LOGGER.info("For more information, visit: https://pypi.org/project/gql/")
    sys.exit(1)

class DictToObject:
    """Helper class to convert dictionary to object with attribute access"""
    
    def __init__(self, data):
        """Initialize with data
        
        Args:
            data: Dictionary or other data to convert
        """
        if isinstance(data, dict):
            for key, value in data.items():
                # Convert camelCase to snake_case for attribute names
                snake_key = self._camel_to_snake(key)
                setattr(self, snake_key, self.__class__(value))
            # Store original dict for direct access if needed
            self._data = data
        elif isinstance(data, list):
            self._data = []
            for item in data:
                self._data.append(self.__class__(item))
            # Make the object itself iterable if it's a list
            self.__iter__ = lambda: iter(self._data)
            self.__getitem__ = lambda i: self._data[i]
            self.__len__ = lambda: len(self._data)
        else:
            self._data = data
    
    def __getattr__(self, name):
        """Handle attribute access for non-existent attributes
        
        Args:
            name: Attribute name
            
        Returns:
            None if attribute doesn't exist
        """
        return None
    
    def get(self, key, default=None):
        """Dict-like get method
        
        Args:
            key: Key to get
            default: Default value if key doesn't exist
            
        Returns:
            Value for key or default
        """
        if hasattr(self, key):
            return getattr(self, key)
        
        # Try snake_case version of the key
        snake_key = self._camel_to_snake(key)
        if hasattr(self, snake_key):
            return getattr(self, snake_key)
        
        return default
    
    def _camel_to_snake(self, name):
        """Convert camelCase to snake_case
        
        Args:
            name: Name to convert
            
        Returns:
            snake_case version of name
        """
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

class MonteCarloClient:
    """Wrapper for Monte Carlo API client"""
    
    def __init__(self, profile=None, mcd_id=None, mcd_token=None):
        """Initialize Monte Carlo client
        
        Args:
            profile (str, optional): Monte Carlo profile name
            mcd_id (str, optional): Monte Carlo ID
            mcd_token (str, optional): Monte Carlo token
        """
        try:
            # Determine the authentication parameters
            if mcd_id and mcd_token:
                # Use provided credentials
                auth_headers = {
                    "X-MCD-ID": mcd_id,
                    "X-MCD-TOKEN": mcd_token
                }
                session_type = "direct"
            elif profile:
                # Get credentials from profile
                config_path = os.path.join(os.path.expanduser("~"), ".mcd", "profiles.ini")
                if os.path.exists(config_path):
                    config = configparser.ConfigParser()
                    config.read(config_path)
                    
                    if profile in config:
                        mcd_id = config[profile].get("mcd_id")
                        mcd_token = config[profile].get("mcd_token")
                        
                        if mcd_id and mcd_token:
                            auth_headers = {
                                "X-MCD-ID": mcd_id,
                                "X-MCD-TOKEN": mcd_token
                            }
                            session_type = f"profile '{profile}'"
                        else:
                            raise ValueError(f"Profile '{profile}' does not contain valid credentials")
                    else:
                        raise ValueError(f"Profile '{profile}' not found in config file")
                else:
                    raise ValueError("No profiles configuration file found")
            else:
                # Try to use default profile
                config_path = os.path.join(os.path.expanduser("~"), ".mcd", "profiles.ini")
                if os.path.exists(config_path):
                    config = configparser.ConfigParser()
                    config.read(config_path)
                    
                    if "default" in config:
                        mcd_id = config["default"].get("mcd_id")
                        mcd_token = config["default"].get("mcd_token")
                        
                        if mcd_id and mcd_token:
                            auth_headers = {
                                "X-MCD-ID": mcd_id,
                                "X-MCD-TOKEN": mcd_token
                            }
                            session_type = "default profile"
                        else:
                            raise ValueError("Default profile does not contain valid credentials")
                    else:
                        raise ValueError("No default profile found in config file")
                else:
                    raise ValueError("No profiles configuration file found")
            
            # Set up the transport with SSL verification disabled
            transport = RequestsHTTPTransport(
                url="https://api.getmontecarlo.com/graphql",
                headers=auth_headers,
                verify=False,  # Disable SSL verification
                retries=3
            )
            
            # Create the GQL client
            self.client = GQLClient(transport=transport)
            LOGGER.info(f"Connected to Monte Carlo API using {session_type}")
            
            # Add a warning about SSL verification being disabled
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            LOGGER.warning("SSL certificate verification is disabled")
            
        except Exception as e:
            LOGGER.error(f"Failed to initialize Monte Carlo client: {str(e)}")
            sys.exit(1)
    
    def execute_query(self, query, variables=None):
        """Execute a GraphQL query
        
        Args:
            query (str): GraphQL query
            variables (dict, optional): Query variables
            
        Returns:
            dict: Query result
        """
        try:
            # Parse the GraphQL query
            gql_query = gql(query)
            
            # Execute the query
            result = self.client.execute(gql_query, variable_values=variables)
            
            # Convert to an object similar to pycarlo's response format for compatibility
            result_obj = DictToObject(result)
            return result_obj
        except Exception as e:
            LOGGER.error(f"Query execution failed: {str(e)}")
            raise

# Utility functions for deep conversion
def deep_dict_convert(obj):
    """
    Recursively convert DictToObject and other custom objects to plain dictionaries
    
    Args:
        obj: Object to convert
        
    Returns:
        Plain dictionary or original object if not convertible
    """
    if obj is None:
        return None
    
    # Handle DictToObject
    if hasattr(obj, '__dict__'):
        return {k: deep_dict_convert(v) for k, v in obj.__dict__.items()}
    
    # Handle objects with to_dict method
    if hasattr(obj, 'to_dict'):
        return deep_dict_convert(obj.to_dict())
    
    # Handle dictionaries
    if isinstance(obj, dict):
        return {k: deep_dict_convert(v) for k, v in obj.items()}
    
    # Handle lists
    if isinstance(obj, list):
        return [deep_dict_convert(item) for item in obj]
    
    # Handle sets
    if isinstance(obj, set):
        return {deep_dict_convert(item) for item in obj}
    
    # Handle tuples
    if isinstance(obj, tuple):
        return tuple(deep_dict_convert(item) for item in obj)
    
    # Return primitive types as is
    return obj