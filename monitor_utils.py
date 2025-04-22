#!/usr/bin/env python3
"""
monitor_utils.py - Utility functions for working with Monte Carlo monitors
This module provides utility functions for managing and updating Monte Carlo monitors.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable

from mc_client import deep_dict_convert
from monitor_types import (
    get_monitor_details, update_validation_monitor,
    update_comparison_monitor, update_stats_monitor,
    pause_unpause_monitor
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
LOGGER = logging.getLogger()

def list_monitors(manager, limit: int = 100, monitor_type: Optional[str] = None) -> List[Dict]:
    """
    List all monitors with optional filtering
    
    Args:
        manager: MonitorManager instance
        limit: Maximum number of monitors to return
        monitor_type: Optional filter by monitor type
        
    Returns:
        List of monitor dictionaries
    """
    monitor_types = [monitor_type] if monitor_type else None
    monitors = manager.get_monitors(limit=limit, monitor_types=monitor_types)
    
    if not monitors:
        LOGGER.info("No monitors found.")
        return []
    
    LOGGER.info(f"Found {len(monitors)} monitors:")
    for i, monitor in enumerate(monitors, 1):
        # Print basic info for each monitor
        LOGGER.info(f"{i}. {monitor.get('name', 'Unnamed')} ({monitor.get('uuid', 'No UUID')})")
        LOGGER.info(f"   Type: {monitor.get('monitorType', 'Unknown')}")
        LOGGER.info(f"   Description: {monitor.get('description', 'No description')}")
        print()
    
    return monitors

def select_monitors_by_uuid(monitors: List[Dict], uuids: List[str]) -> List[Dict]:
    """
    Select monitors from a list by their UUIDs
    
    Args:
        monitors: List of monitor dictionaries
        uuids: List of UUIDs to select
        
    Returns:
        List of selected monitor dictionaries
    """
    selected_monitors = []
    uuid_map = {m.get('uuid'): m for m in monitors if 'uuid' in m}
    
    for uuid in uuids:
        if uuid in uuid_map:
            selected_monitors.append(uuid_map[uuid])
        else:
            LOGGER.warning(f"Warning: Monitor with UUID '{uuid}' not found.")
    
    return selected_monitors

def update_monitor(manager, monitor: Dict, updates: Dict) -> bool:
    """
    Update a monitor with new configuration
    
    Args:
        manager: MonitorManager instance
        monitor: Monitor dictionary with configuration
        updates: Dictionary with fields to update
        
    Returns:
        Boolean indicating success
    """
    uuid = monitor.get('uuid')
    monitor_type = monitor.get('monitorType')
    
    if not uuid:
        LOGGER.error("Error: Monitor has no UUID.")
        return False
    
    # Apply updates to the monitor configuration
    updated_config = {**monitor}
    for key, value in updates.items():
        updated_config[key] = value
    
    updated_config['uuid'] = uuid  # Ensure UUID is included
    
    result = None
    
    # Use the appropriate update method based on monitor type
    if monitor_type == 'CUSTOM_SQL':
        result = manager.create_or_update_custom_sql_rule(updated_config)
    elif monitor_type == 'VALIDATION':
        result = update_validation_monitor(manager, updated_config)
    elif monitor_type == 'COMPARISON':
        result = update_comparison_monitor(manager, updated_config)
    elif monitor_type in ['STATS', 'METRIC']:
        result = update_stats_monitor(manager, updated_config)
    else:
        LOGGER.error(f"Error: Updating {monitor_type} monitors is not yet implemented.")
        return False
    
    if result:
        LOGGER.info(f"Successfully updated monitor: {uuid}")
        return True
    else:
        LOGGER.error(f"Failed to update monitor: {uuid}")
        return False

def bulk_update_monitors(manager, monitors: List[Dict], update_fn: Callable):
    """
    Apply an update function to multiple monitors
    
    Args:
        manager: MonitorManager instance
        monitors: List of monitor dictionaries
        update_fn: Function that takes a monitor and returns update dictionary
    """
    success_count = 0
    fail_count = 0
    
    for monitor in monitors:
        uuid = monitor.get('uuid')
        LOGGER.info(f"\nProcessing monitor: {uuid}")
        
        # Get detailed configuration
        detailed_config = get_monitor_details(manager, monitor)
        if not detailed_config:
            LOGGER.error(f"Skipping monitor due to missing details: {uuid}")
            fail_count += 1
            continue
        
        # Get updates from the update function
        updates = update_fn(detailed_config)
        if not updates:
            LOGGER.info(f"No updates required for: {uuid}")
            continue
        
        # Apply the updates
        if update_monitor(manager, detailed_config, updates):
            success_count += 1
        else:
            fail_count += 1
    
    LOGGER.info(f"\nBulk update complete. {success_count} succeeded, {fail_count} failed.")

def fill_template_interactively(template: Dict) -> Dict:
    """
    Fill a template interactively with user input
    
    Args:
        template: Template dictionary
        
    Returns:
        Dictionary with user updates
    """
    updates = {}
    
    print("\n=== INTERACTIVE UPDATE TEMPLATE ===")
    print("Enter values for fields you want to update (leave blank to skip):")
    
    # Process each top-level field in the template
    for field_name, field_config in template.items():
        print(f"\n--- {field_name} ---")
        
        # Handle description field (simple string)
        if field_name == "description":
            response = input(f"Update description? (y/n): ")
            if response.lower() == 'y':
                new_value = input(f"Enter new description: ")
                updates[field_name] = new_value
                continue
        
        # Handle scheduleConfig field
        elif field_name == "scheduleConfig":
            response = input(f"Update schedule? (y/n): ")
            if response.lower() == 'y':
                schedule = {}
                
                interval = input(f"Enter interval in minutes (e.g., 1440 for daily, blank for default): ")
                if interval:
                    schedule["intervalMinutes"] = int(interval)
                else:
                    schedule["intervalMinutes"] = field_config["value"]["intervalMinutes"]
                
                # Use default start time (2 AM UTC)
                schedule["scheduleType"] = "FIXED"
                schedule["startTime"] = field_config["value"]["startTime"]
                
                updates[field_name] = schedule
                continue
        
        # Handle alertConfig for VALIDATION monitors
        elif field_name == "alertConfig":
            response = input(f"Update alert config? (y/n): ")
            if response.lower() == 'y':
                print("Note: Direct API support for alert configuration updates is limited.")
                print("You may need to use the Monte Carlo UI for alert configuration changes.")
                alert_config = {}
                
                alert_on_diff = input(f"Alert on difference? (true/false, blank for default): ")
                if alert_on_diff:
                    alert_config["alertOnDiff"] = alert_on_diff.lower() == 'true'
                else:
                    alert_config["alertOnDiff"] = field_config["value"]["alertOnDiff"]
                
                threshold = input(f"Enter difference threshold % (blank for default): ")
                if threshold:
                    alert_config["diffThreshold"] = float(threshold)
                else:
                    alert_config["diffThreshold"] = field_config["value"]["diffThreshold"]
                
                updates[field_name] = alert_config
                continue
        
        # Handle alertCondition for COMPARISON and CUSTOM_SQL monitors
        elif field_name == "alertCondition":
            response = input(f"Update alert condition? (y/n): ")
            if response.lower() == 'y':
                print("Note: Direct API support for alert condition updates is limited.")
                print("You may need to use the Monte Carlo UI for alert condition changes.")
                alert_condition = {}
                
                # Use default values from template
                alert_condition["type"] = field_config["value"]["type"]
                alert_condition["operator"] = field_config["value"]["operator"]
                
                threshold = input(f"Enter threshold value (blank for default): ")
                if threshold:
                    alert_condition["threshold"] = float(threshold)
                else:
                    alert_condition["threshold"] = field_config["value"]["threshold"]
                
                updates[field_name] = alert_condition
                continue
        
        # Handle alertConditions for METRIC/STATS monitors
        elif field_name == "alertConditions":
            response = input(f"Update alert conditions? (y/n): ")
            if response.lower() == 'y':
                print("Note: Direct API support for alert conditions updates is limited.")
                print("You may need to use the Monte Carlo UI for alert condition changes.")
                # Use default values for now - this could be expanded for more customization
                updates[field_name] = field_config["value"]
                continue
    
    return updates

def create_update_template(monitor_type: str) -> Dict:
    """
    Create an update template based on monitor type
    
    Args:
        monitor_type: Type of monitor (VALIDATION, CUSTOM_SQL, etc.)
        
    Returns:
        Dictionary with template fields for updates
    """
    # Common fields for all monitor types
    template = {
        "description": {
            "enabled": False,
            "value": ""
        },
        "scheduleConfig": {
            "enabled": False,
            "value": {
                "scheduleType": "FIXED",
                "intervalMinutes": 1440,  # Daily
                "startTime": datetime.utcnow().replace(hour=2, minute=0, second=0).isoformat() + 'Z'
            }
        }
    }
    
    # Add monitor-specific fields
    if monitor_type == 'VALIDATION':
        template["alertConfig"] = {
            "enabled": False,
            "value": {
                "alertOnDiff": True,
                "diffThreshold": 5  # 5% difference threshold
            }
        }
    elif monitor_type == 'COMPARISON':
        template["alertCondition"] = {
            "enabled": False,
            "value": {
                "type": "THRESHOLD",
                "operator": "GT",
                "threshold": 10
            }
        }
    elif monitor_type in ['METRIC', 'STATS']:
        template["alertConditions"] = {
            "enabled": False,
            "value": [
                {
                    "type": "threshold",
                    "operator": "AUTO",
                    "metric": "ROW_COUNT_CHANGE",
                    "fields": []
                }
            ]
        }
    elif monitor_type == 'CUSTOM_SQL':
        template["alertCondition"] = {
            "enabled": False,
            "value": {
                "type": "THRESHOLD",
                "operator": "GT",
                "threshold": 0
            }
        }
    
    return template

def apply_template_updates(monitor: Dict, updates: Dict) -> Dict:
    """
    Apply template updates to a monitor configuration
    
    Args:
        monitor: Monitor configuration dictionary
        updates: Updates dictionary from template
        
    Returns:
        Dictionary with updates to apply
    """
    # Filter out empty updates
    return {k: v for k, v in updates.items() if v is not None}

# Example update functions that can be used with bulk_update_monitors
def update_schedule_example(monitor: Dict) -> Dict:
    """
    Example update function: Update monitor schedule
    
    Args:
        monitor: Monitor dictionary with configuration
        
    Returns:
        Dictionary with schedule updates
    """
    # Example: Change the schedule to run at 2 AM UTC daily
    new_start_time = datetime.utcnow().replace(hour=2, minute=0, second=0).isoformat() + 'Z'
    
    if 'scheduleConfig' in monitor:
        return {
            'scheduleConfig': {
                'scheduleType': 'FIXED',
                'intervalMinutes': 1440,  # Daily
                'startTime': new_start_time
            }
        }
    else:
        return {
            'scheduleConfig': {
                'scheduleType': 'FIXED',
                'intervalMinutes': 1440,  # Daily
                'startTime': new_start_time
            }
        }

def update_description_example(monitor: Dict) -> Dict:
    """
    Example update function: Update monitor description
    
    Args:
        monitor: Monitor dictionary with configuration
        
    Returns:
        Dictionary with description update
    """
    current_description = monitor.get('description', '')
    new_description = f"{current_description} (Updated on {datetime.now().strftime('%Y-%m-%d')})"
    
    return {
        'description': new_description
    }

def update_alert_thresholds_example(monitor: Dict) -> Dict:
    """
    Example update function: Update alert thresholds
    
    Args:
        monitor: Monitor dictionary with configuration
        
    Returns:
        Dictionary with alert threshold updates
    """
    monitor_type = monitor.get('monitorType')
    
    # For VALIDATION monitors
    if monitor_type == 'VALIDATION' and 'alertConfig' in monitor:
        return {
            'alertConfig': {
                'alertOnDiff': True,
                'diffThreshold': 5  # 5% difference threshold
            }
        }
    
    # For COMPARISON monitors
    elif monitor_type == 'COMPARISON' and 'alertCondition' in monitor:
        return {
            'alertCondition': {
                'type': 'THRESHOLD',
                'operator': 'GT',
                'threshold': 10
            }
        }
    
    # For METRIC/STATS monitors
    elif monitor_type in ['METRIC', 'STATS'] and 'alertConditions' in monitor:
        return {
            'alertConditions': [
                {
                    'type': 'threshold',
                    'operator': 'AUTO',
                    'metric': 'ROW_COUNT_CHANGE',
                    'fields': []
                }
            ]
        }
    
    # For CUSTOM_SQL monitors
    elif monitor_type == 'CUSTOM_SQL' and 'alertCondition' in monitor:
        return {
            'alertCondition': {
                'type': 'THRESHOLD',
                'operator': 'GT',
                'threshold': 0
            }
        }
    
    LOGGER.info(f"No alert threshold update applicable for this monitor type: {monitor_type}")
    return {}

def get_graphql_schema(mc_client):
    """
    Retrieve the full GraphQL schema using introspection and save mutations
    
    Args:
        mc_client: MonteCarloClient instance
    
    Returns:
        Full GraphQL schema as a dictionary
    """
    introspection_query = """
    query IntrospectionQuery {
      __schema {
        queryType {
          name
          fields {
            name
            description
            args {
              name
              description
              type {
                name
                kind
                ofType {
                  name
                  kind
                }
              }
            }
          }
        }
        mutationType {
          name
          fields {
            name
            description
            args {
              name
              description
              type {
                name
                kind
                ofType {
                  name
                  kind
                }
              }
            }
          }
        }
        types {
          name
          kind
          description
          fields {
            name
            description
            type {
              name
              kind
              ofType {
                name
                kind
              }
            }
          }
          inputFields {
            name
            description
            type {
              name
              kind
              ofType {
                name
                kind
              }
            }
          }
        }
      }
    }
    """
    
    try:
        result = mc_client.execute_query(introspection_query)
        
        # Save the schema to a file for easier inspection
        
        # Save full schema
        with open('mc_graphql_schema.json', 'w') as f:
            json.dump(deep_dict_convert(result), f, indent=2)
        
        LOGGER.info(f"Schema saved to mc_graphql_schema.json")
        
        # Extract and save only mutations for easier reference
        schema_dict = deep_dict_convert(result)
        if 'data' in schema_dict and '__schema' in schema_dict['data']:
            schema = schema_dict['data']['__schema']
            
            if 'mutationType' in schema and schema['mutationType'] and 'fields' in schema['mutationType']:
                mutations = schema['mutationType']['fields']
                
                # Save mutations to a separate file
                with open('mc_mutations.json', 'w') as f:
                    json.dump(mutations, f, indent=2)
                
                LOGGER.info(f"Mutations saved to mc_mutations.json")
                
                # Print monitor and alert related mutations
                monitor_mutations = [
                    f for f in mutations 
                    if any(term in f["name"].lower() for term in ["monitor", "alert", "rule"])
                ]
                
                LOGGER.info("\nAvailable Monitor/Alert/Rule Mutations:")
                for mutation in monitor_mutations:
                    args = [arg["name"] for arg in mutation.get("args", [])]
                    LOGGER.info(f"- {mutation['name']}({', '.join(args)})")
            
            # Print input types related to monitors and alerts
            if "types" in schema:
                input_types = [
                    t for t in schema["types"]
                    if t["kind"] == "INPUT_OBJECT" and 
                    any(term in t["name"].lower() for term in ["monitor", "alert", "rule"])
                ]
                
                LOGGER.info("\nMonitor/Alert/Rule-related Input Types:")
                for t in input_types:
                    fields = []
                    if "inputFields" in t and t["inputFields"]:
                        fields = [f["name"] for f in t["inputFields"]]
                    elif "fields" in t and t["fields"]:
                        fields = [f["name"] for f in t["fields"]]
                    
                    LOGGER.info(f"- {t['name']}: {fields}")
        
        return deep_dict_convert(result)
    except Exception as e:
        LOGGER.error(f"Error retrieving GraphQL schema: {e}")
        return None