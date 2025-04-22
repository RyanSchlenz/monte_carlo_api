#!/usr/bin/env python3
"""
monitor_types.py - Monitor type-specific functions and utilities
This module provides functions for handling different monitor types in Monte Carlo.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import time

from mc_client import deep_dict_convert

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
LOGGER = logging.getLogger()

def get_validation_rule(manager, uuid: str) -> Dict:
    """
    Get detailed configuration for a validation rule
    
    Args:
        manager: MonitorManager instance
        uuid: UUID of the validation rule
        
    Returns:
        Dictionary with rule configuration
    """
    # First try to get basic info using getMonitors query
    monitors = manager.get_monitors(uuids=[uuid])
    
    if not monitors or len(monitors) == 0:
        LOGGER.error(f"Monitor with UUID {uuid} not found")
        return {}
    
    monitor = monitors[0]
    
    # For VALIDATION monitors, start with basic info
    validation_config = {
        "uuid": uuid,
        "name": monitor.get("name", ""),
        "description": monitor.get("description", ""),
        "monitorType": monitor.get("monitorType", "VALIDATION")
    }
    
    # Use the query to get more details
    query = """
    query getMonitorByUuid($uuids: [String]) {
      getMonitors(uuids: $uuids) {
        uuid
        name
        description
        monitorType
        consolidatedMonitorStatus
        dataQualityDimension
        createdTime
        prevExecutionTime
      }
    }
    """
    
    try:
        result = manager.mc_client.execute_query(query, {"uuids": [uuid]})
        
        if result and "data" in result._data and "getMonitors" in result._data["data"] and result._data["data"]["getMonitors"]:
            details = result._data["data"]["getMonitors"][0]
            validation_config.update(details)
            LOGGER.info(f"Retrieved details for Validation Monitor: {uuid}")
            return validation_config
    except Exception as e:
        LOGGER.error(f"Error getting validation details: {str(e)}")
        # Continue with basic info if detailed query fails
    
    # If we couldn't get detailed info, return what we have
    LOGGER.info(f"Retrieved basic info for Validation Monitor: {uuid}")
    return validation_config

def get_comparison_rule(manager, uuid: str) -> Dict:
    """
    Get detailed configuration for a comparison rule
    
    Args:
        manager: MonitorManager instance
        uuid: UUID of the comparison rule
        
    Returns:
        Dictionary with rule configuration
    """
    # Prepare the query to get comparison rule details using getMonitors
    query = """
    query getComparisonRule($uuids: [String]) {
      getMonitors(uuids: $uuids) {
        uuid
        name
        description
        monitorType
        consolidatedMonitorStatus
        dataQualityDimension
        createdTime
        prevExecutionTime
      }
    }
    """
    
    # Execute the query
    result = manager.mc_client.execute_query(query, {"uuids": [uuid]})
    
    if result and "data" in result._data and "getMonitors" in result._data["data"] and result._data["data"]["getMonitors"]:
        rule_dict = result._data["data"]["getMonitors"][0]
        return rule_dict
    
    return {}

def get_stats_rule(manager, uuid: str) -> Dict:
    """
    Get detailed configuration for a stats/metric rule
    
    Args:
        manager: MonitorManager instance
        uuid: UUID of the stats rule
        
    Returns:
        Dictionary with rule configuration
    """
    # Use the query to get a metric monitor by UUID
    query = """
    query getMetricMonitor($uuids: [String]) {
      getMonitors(uuids: $uuids) {
        uuid
        name
        description
        monitorType
        consolidatedMonitorStatus
        dataQualityDimension
        createdTime
        prevExecutionTime
      }
    }
    """
    
    # Execute the query
    result = manager.mc_client.execute_query(query, {"uuids": [uuid]})
    
    if result and "data" in result._data and "getMonitors" in result._data["data"] and result._data["data"]["getMonitors"]:
        monitor_dict = result._data["data"]["getMonitors"][0]
        return monitor_dict
    
    return {}

def get_monitor_details(manager, monitor: Dict) -> Dict:
    """
    Get detailed configuration for a monitor
    
    Args:
        manager: MonitorManager instance
        monitor: Monitor dictionary with basic info
        
    Returns:
        Dictionary with detailed monitor configuration
    """
    uuid = monitor.get('uuid')
    monitor_type = monitor.get('monitorType')
    
    if not uuid:
        LOGGER.error("Error: Monitor has no UUID.")
        return {}
    
    if monitor_type == 'CUSTOM_SQL':
        details = manager.get_custom_rule(uuid)
        if details:
            LOGGER.info(f"Retrieved details for Custom SQL Monitor: {uuid}")
            return details
    elif monitor_type == 'VALIDATION':
        details = get_validation_rule(manager, uuid)
        if details:
            LOGGER.info(f"Retrieved details for Validation Monitor: {uuid}")
            return details
    elif monitor_type == 'COMPARISON':
        details = get_comparison_rule(manager, uuid)
        if details:
            LOGGER.info(f"Retrieved details for Comparison Monitor: {uuid}")
            return details
    elif monitor_type in ['STATS', 'METRIC']:
        details = get_stats_rule(manager, uuid)
        if details:
            LOGGER.info(f"Retrieved details for {monitor_type} Monitor: {uuid}")
            return details
    else:
        # For other monitor types, return the basic info we have
        LOGGER.info(f"Getting details for {monitor_type} monitor is not yet implemented.")
        return monitor
    
    LOGGER.error(f"Error: Could not retrieve details for monitor: {uuid}")
    return {}

def pause_unpause_monitor(manager, uuid: str) -> bool:
    """
    Pause and then unpause a monitor
    
    Args:
        manager: MonitorManager instance
        uuid: UUID of the monitor
        
    Returns:
        Boolean indicating success
    """
    try:
        # The mutation for pausing/unpausing a monitor
        pause_mutation = """
        mutation pauseMonitor($uuid: UUID!, $pause: Boolean!) {
          pauseMonitor(uuid: $uuid, pause: $pause) {
            uuid
          }
        }
        """
        
        # First pause
        LOGGER.info(f"Pausing monitor: {uuid}")
        result1 = manager.mc_client.execute_query(pause_mutation, {"uuid": uuid, "pause": True})
        
        # Convert to dict if it's a DictToObject
        result1_dict = deep_dict_convert(result1)
        
        # Improved debug information
        LOGGER.info(f"Pause response: {result1_dict}")
        
        # Wait a moment
        time.sleep(2)
        
        # Then unpause
        LOGGER.info(f"Unpausing monitor: {uuid}")
        result2 = manager.mc_client.execute_query(pause_mutation, {"uuid": uuid, "pause": False})
        
        # Convert to dict if it's a DictToObject
        result2_dict = deep_dict_convert(result2)
        
        # Improved debug information
        LOGGER.info(f"Unpause response: {result2_dict}")
        
        # Check if both responses are successful (no errors)
        pause_success = (result1_dict is not None and 
                        isinstance(result1_dict, dict) and
                        "errors" not in result1_dict)
        
        unpause_success = (result2_dict is not None and 
                          isinstance(result2_dict, dict) and
                          "errors" not in result2_dict)
        
        if pause_success and unpause_success:
            LOGGER.info(f"Successfully paused and unpaused monitor: {uuid}")
            return True
        else:
            LOGGER.error(f"Failed to pause/unpause monitor: {uuid}")
            return False
    
    except Exception as e:
        LOGGER.error(f"Error pausing/unpausing monitor: {str(e)}")
        return False

def update_validation_monitor(manager, config: Dict) -> Dict:
    """
    Update a validation monitor using available API endpoints
    
    Args:
        manager: MonitorManager instance
        config: Monitor configuration dictionary
        
    Returns:
        Updated monitor details or empty dict on failure
    """
    uuid = config.get('uuid')
    if not uuid:
        LOGGER.error("Error: Monitor has no UUID.")
        return {}
    
    # Get original description if available
    original_description = config.get('description', '')
    
    # Track if any operation was attempted and its success
    operations_attempted = 0
    success_count = 0
    
    # Enhanced debug printing
    LOGGER.info(f"Attempting to update monitor {uuid}")
    
    # Clone the config to avoid modifying the original
    config_copy = deep_dict_convert(config)
    LOGGER.info(f"Current config: {json.dumps(config_copy, indent=2)}")
    
    # Try to update the schedule 
    if 'scheduleConfig' in config:
        operations_attempted += 1
        try:
            mutation = """
            mutation updateMonitorsSchedules($monitorUuids: [UUID!]!, $scheduleConfig: ScheduleConfigInput!) {
              updateMonitorsSchedules(
                monitorUuids: $monitorUuids
                scheduleConfig: $scheduleConfig
              ) {
                success
              }
            }
            """
            
            # Prepare schedule input
            schedule_config = deep_dict_convert(config['scheduleConfig'])
            
            # Create a clean input without any custom objects
            schedule_input = {
                "monitorUuids": [uuid],
                "scheduleConfig": {
                    "scheduleType": schedule_config.get('scheduleType', 'FIXED'),
                    "intervalMinutes": schedule_config.get('intervalMinutes', 1440),
                    "startTime": schedule_config.get('startTime', 
                        datetime.utcnow().replace(hour=2, minute=0, second=0).isoformat() + 'Z')
                }
            }
            
            # Execute the mutation
            result = manager.mc_client.execute_query(mutation, schedule_input)
            
            # Convert result to plain dict
            result_dict = deep_dict_convert(result)
            
            LOGGER.info("Schedule update result: " + json.dumps(result_dict, indent=2))
            
            # Improved check for success in various possible response structures
            schedule_success = False
            
            # Check multiple possible paths where success might be found
            if result_dict:
                # Direct path in data structure
                if 'data' in result_dict and 'updateMonitorsSchedules' in result_dict['data']:
                    if 'success' in result_dict['data']['updateMonitorsSchedules']:
                        schedule_success = result_dict['data']['updateMonitorsSchedules']['success']
                
                # Alternative path with _data
                if '_data' in result_dict and 'updateMonitorsSchedules' in result_dict['_data']:
                    if 'success' in result_dict['_data']['updateMonitorsSchedules']:
                        schedule_success = result_dict['_data']['updateMonitorsSchedules']['success']
                
                # Check for update_monitors_schedules path
                if 'update_monitors_schedules' in result_dict:
                    update_path = result_dict['update_monitors_schedules']
                    if isinstance(update_path, dict):
                        if 'success' in update_path:
                            if isinstance(update_path['success'], dict) and '_data' in update_path['success']:
                                schedule_success = update_path['success']['_data']
                            else:
                                schedule_success = update_path['success']
                        elif '_data' in update_path and 'success' in update_path['_data']:
                            schedule_success = update_path['_data']['success']
            
            # Convert to boolean
            schedule_success = bool(schedule_success)
            
            if schedule_success:
                LOGGER.info(f"Successfully updated schedule for validation monitor: {uuid}")
                success_count += 1
            else:
                LOGGER.error(f"Failed to update schedule for validation monitor: {uuid}")
        except Exception as e:
            LOGGER.error(f"Error updating validation monitor schedule: {str(e)}")
    
    # Handle description update with improved error handling
    if 'description' in config:
        operations_attempted += 1
        try:
            mutation = """
            mutation updateMonitorDescription($monitorUuid: UUID!, $description: String!) {
              updateMonitorDescription(monitorUuid: $monitorUuid, description: $description) {
                success
              }
            }
            """
            
            description_input = {
                "monitorUuid": uuid,
                "description": config['description']
            }
            
            result = manager.mc_client.execute_query(mutation, description_input)
            
            # Convert to a plain dict
            result_dict = deep_dict_convert(result)
            
            LOGGER.info("Description update result: " + json.dumps(result_dict, indent=2))
            
            # Improved check for success in various possible response structures
            description_success = False
            
            # Check multiple possible paths where success might be found
            if result_dict:
                # Direct path in data structure
                if 'data' in result_dict and 'updateMonitorDescription' in result_dict['data']:
                    if 'success' in result_dict['data']['updateMonitorDescription']:
                        description_success = result_dict['data']['updateMonitorDescription']['success']
                
                # Alternative path with _data
                if '_data' in result_dict and 'updateMonitorDescription' in result_dict['_data']:
                    if 'success' in result_dict['_data']['updateMonitorDescription']:
                        description_success = result_dict['_data']['updateMonitorDescription']['success']
                
                # Check for update_monitor_description path
                if 'update_monitor_description' in result_dict:
                    update_path = result_dict['update_monitor_description']
                    if isinstance(update_path, dict):
                        if 'success' in update_path:
                            if isinstance(update_path['success'], dict) and '_data' in update_path['success']:
                                description_success = update_path['success']['_data']
                            else:
                                description_success = update_path['success']
                        elif '_data' in update_path and 'success' in update_path['_data']:
                            description_success = update_path['_data']['success']
            
            # Convert to boolean
            description_success = bool(description_success)
            
            if description_success:
                LOGGER.info(f"Successfully updated description for validation monitor: {uuid}")
                success_count += 1
            else:
                LOGGER.error(f"Failed to update description for validation monitor: {uuid}")
        except Exception as e:
            LOGGER.error(f"Error updating validation monitor description: {str(e)}")
    
    # Consider the update successful if any operation succeeded
    if operations_attempted > 0 and success_count > 0:
        LOGGER.info(f"Update successful: {success_count} of {operations_attempted} operations succeeded")
        return {
            "uuid": uuid, 
            "description": config.get('description', original_description),
            "scheduleConfig": deep_dict_convert(config.get('scheduleConfig', {}))
        }
    else:
        if operations_attempted == 0:
            LOGGER.warning(f"No update operations were attempted for monitor: {uuid}")
        else:
            LOGGER.error(f"Failed to update validation monitor: {uuid}")
        return {}

def update_comparison_monitor(manager, config: Dict) -> Dict:
    """
    Update a comparison monitor
    
    Args:
        manager: MonitorManager instance
        config: Monitor configuration dictionary
        
    Returns:
        Updated monitor details or empty dict on failure
    """
    # Prepare the mutation for comparison monitor with required sub-selection
    mutation = """
    mutation createOrUpdateComparisonRule($input: CreateOrUpdateComparisonRuleInput!) {
      createOrUpdateComparisonRule(input: $input) {
        comparisonRule {
          uuid
        }
      }
    }
    """
    
    # Convert config to plain dict
    input_config = deep_dict_convert(config)
    
    # Execute the mutation
    result = manager.mc_client.execute_query(mutation, {"input": input_config})
    result_dict = deep_dict_convert(result)
    
    try:
        if result_dict is not None and "errors" not in result_dict:
            LOGGER.info(f"Updated comparison rule: {config.get('uuid')}")
            return {
                'uuid': config.get('uuid'),
                'description': config.get('description')
            }
    except Exception as e:
        LOGGER.error(f"Error parsing comparison rule result: {str(e)}")
    
    LOGGER.error(f"Failed to update comparison rule: {config.get('uuid')}")
    return {}

def update_stats_monitor(manager, config: Dict) -> Dict:
    """
    Update a stats/metric monitor
    
    Args:
        manager: MonitorManager instance
        config: Monitor configuration dictionary
        
    Returns:
        Updated monitor details or empty dict on failure
    """
    # Convert config to plain dict before passing to manager
    input_config = deep_dict_convert(config)
    return manager.create_or_update_metric_monitor(input_config)