#!/usr/bin/env python3
"""
monitor_manager.py - Monte Carlo Monitor Manager
This module provides a wrapper for managing Monte Carlo monitors via the GraphQL API.
"""

import logging
from typing import Dict, List, Optional, Any

from mc_client import MonteCarloClient, deep_dict_convert

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
LOGGER = logging.getLogger()

class MonitorManager:
    """Class for managing Monte Carlo monitors"""
    
    def __init__(self, mc_client):
        """Initialize the monitor manager
        
        Args:
            mc_client (MonteCarloClient): Monte Carlo client
        """
        self.mc_client = mc_client
    
    def get_monitors(self, limit=100, monitor_types=None, mcons=None, uuids=None, 
                    labels=None, tags=None, domain_id=None, consolidated_status_types=None,
                    include_ootb_monitors=None, alerted_only=None):
        """Get a list of monitors
        
        Args:
            limit (int, optional): Maximum number of monitors to return
            monitor_types (list, optional): Types of monitors to return
            mcons (list, optional): MCONs to filter by
            uuids (list, optional): UUIDs to filter by
            labels (list, optional): Labels to filter by
            tags (list, optional): Tags to filter by
            domain_id (str, optional): Domain ID to filter by
            consolidated_status_types (list, optional): Status types to filter by
            include_ootb_monitors (bool, optional): Whether to include out-of-the-box monitors
            alerted_only (bool, optional): Whether to only include monitors with alerts
            
        Returns:
            list: List of monitors
        """
        # Prepare the query
        query = """
        query getMonitors($alertedOnly: Boolean,
                        $consolidatedStatusTypes: [ConsolidatedMonitorStatusType],
                        $domainId: UUID,
                        $includeOotbMonitors: Boolean,
                        $labels: [String],
                        $limit: Int,
                        $mcons: [String],
                        $monitorTypes: [UserDefinedMonitors],
                        $tags: [TagKeyValuePairInput!],
                        $uuids: [String]) {
        getMonitors(alertedOnly: $alertedOnly,
                    consolidatedStatusTypes: $consolidatedStatusTypes,
                    domainId: $domainId,
                    includeOotbMonitors: $includeOotbMonitors,
                    labels: $labels,
                    limit: $limit,
                    mcons: $mcons,
                    monitorTypes: $monitorTypes,
                    tags: $tags,
                    uuids: $uuids) {
            uuid
            monitorType
            name
            description
            prevExecutionTime
            dataQualityDimension
            alertIds
            createdTime
            consolidatedMonitorStatus
        }
        }
        """
        
        # Prepare variables
        variables = {"limit": limit}
        
        if monitor_types:
            variables["monitorTypes"] = monitor_types
        
        if mcons:
            variables["mcons"] = mcons
        
        if uuids:
            variables["uuids"] = uuids
        
        if labels:
            variables["labels"] = labels
        
        if tags:
            variables["tags"] = tags
        
        if domain_id:
            variables["domainId"] = domain_id
        
        if consolidated_status_types:
            variables["consolidatedStatusTypes"] = consolidated_status_types
        
        if include_ootb_monitors is not None:
            variables["includeOotbMonitors"] = include_ootb_monitors
        
        if alerted_only is not None:
            variables["alertedOnly"] = alerted_only
        
        # Execute the query
        result = self.mc_client.execute_query(query, variables)
        
        # Log the query and variables for debugging
        LOGGER.info(f"Query variables: {variables}")
        
        # Direct access to the returned data structure
        monitors = []
        
        if hasattr(result, '_data'):
            data = result._data
            if isinstance(data, dict) and 'data' in data and 'getMonitors' in data['data']:
                monitors_data = data['data']['getMonitors']
                if monitors_data and isinstance(monitors_data, list):
                    monitors = monitors_data
        
        # If we still don't have monitors, try other access methods
        if not monitors:
            if hasattr(result, 'data') and result.data:
                if hasattr(result.data, 'get_monitors'):
                    monitors_data = result.data.get_monitors
                    if monitors_data and hasattr(monitors_data, '_data') and isinstance(monitors_data._data, list):
                        monitors = monitors_data._data
            
            # One more approach - check for get_monitors attribute
            if hasattr(result, 'get_monitors'):
                monitors_data = result.get_monitors
                if monitors_data and hasattr(monitors_data, '_data') and isinstance(monitors_data._data, list):
                    monitors = monitors_data._data
        
        # Fallback to empty list if no data found
        if not monitors:
            LOGGER.error("Failed to get monitors")
            return []
        
        # Process the monitors for consistent dictionary format
        processed_monitors = []
        for monitor in monitors:
            # If monitor is already a dict, use it directly
            if isinstance(monitor, dict):
                processed_monitors.append(monitor)
            # If monitor is a DictToObject, convert it to a dict
            elif hasattr(monitor, '_data'):
                processed_monitors.append(monitor._data)
            # Otherwise, try to convert it to a dict
            else:
                processed_monitors.append(deep_dict_convert(monitor))
        
        return processed_monitors

    def get_custom_rule(self, rule_id):
        """Get details of a Custom SQL Monitor
        
        Args:
            rule_id (str): UUID of the custom rule
            
        Returns:
            dict: Custom rule details
        """
        # Prepare the query
        query = """
        query getCustomRule($ruleId: String!) {
          getCustomRule(ruleId: $ruleId) {
            intervalMinutes
            comparisons {
              comparisonType
              metric
              operator
              threshold
            }
            description
            timezone
            startTime
            customSql
          }
        }
        """
        
        # Execute the query
        result = self.mc_client.execute_query(query, {"ruleId": rule_id})
        
        if hasattr(result, 'get_custom_rule') and result.get_custom_rule:
            return deep_dict_convert(result.get_custom_rule)
        
        LOGGER.error(f"Failed to get custom rule: {rule_id}")
        return None
    
    def create_or_update_custom_sql_rule(self, params):
        """Create or update a Custom SQL Monitor
        
        Args:
            params (dict): Parameters for the custom SQL monitor
            
        Returns:
            dict: Created or updated monitor info
        """
        # Extract required parameters
        description = params.get('description')
        dw_id = params.get('dwId')
        sql = params.get('sql')
        schedule_config = params.get('scheduleConfig')
        alert_condition = params.get('alertCondition')
        uuid = params.get('uuid')
        
        # Validate required parameters
        if not all([description, dw_id, sql, schedule_config, alert_condition]):
            LOGGER.error("Missing required parameters for custom SQL monitor")
            return None
        
        # Prepare the mutation
        mutation = """
        mutation createOrUpdateCustomSqlRule($input: CreateOrUpdateCustomSqlRuleInput!) {
          createOrUpdateCustomSqlRule(input: $input) {
            customRule {
              uuid
              creatorId
              description
            }
          }
        }
        """
        
        # Prepare input variables
        input_vars = {
            "description": description,
            "dwId": dw_id,
            "sql": sql,
            "scheduleConfig": schedule_config,
            "alertCondition": alert_condition
        }
        
        # Add UUID if updating an existing monitor
        if uuid:
            input_vars["uuid"] = uuid
        
        # Execute the mutation
        result = self.mc_client.execute_query(mutation, {"input": input_vars})
        
        if hasattr(result, 'create_or_update_custom_sql_rule') and result.create_or_update_custom_sql_rule:
            rule = result.create_or_update_custom_sql_rule.custom_rule
            LOGGER.info(f"{'Updated' if uuid else 'Created'} custom SQL rule: {rule.uuid}")
            return deep_dict_convert(rule)
        
        LOGGER.error(f"Failed to {'update' if uuid else 'create'} custom SQL rule")
        return None
    
    def create_or_update_metric_monitor(self, params):
        """Create or update a Metric Monitor
        
        Args:
            params (dict): Parameters for the metric monitor
            
        Returns:
            dict: Created or updated monitor info
        """
        # Convert params to plain dict
        input_params = deep_dict_convert(params)
        
        # Prepare the mutation
        mutation = """
        mutation createOrUpdateMetricMonitor($input: CreateOrUpdateMetricMonitorInput!) {
          createOrUpdateMetricMonitor(input: $input) {
            metricMonitor {
              uuid
              name
              description
              createdBy {
                email
              }
            }
          }
        }
        """
        
        # Execute the mutation
        result = self.mc_client.execute_query(mutation, {"input": input_params})
        
        if hasattr(result, 'create_or_update_metric_monitor') and result.create_or_update_metric_monitor:
            monitor = result.create_or_update_metric_monitor.metric_monitor
            LOGGER.info(f"{'Updated' if 'uuid' in params else 'Created'} metric monitor: {monitor.uuid}")
            return deep_dict_convert(monitor)
        
        LOGGER.error(f"Failed to {'update' if 'uuid' in params else 'create'} metric monitor")
        return None
        
    def get_job_executions(self, monitor_uuid=None, custom_rule_uuid=None, 
                          history_days=30, first=100, cursor=None):
        """Get monitor run executions
        
        Args:
            monitor_uuid (str, optional): UUID of the monitor
            custom_rule_uuid (str, optional): UUID of the custom rule
            history_days (int, optional): Number of days of history to return
            first (int, optional): Maximum number of executions to return
            cursor (str, optional): Cursor for pagination
            
        Returns:
            dict: Dictionary with executions list and pagination info
        """
        # Prepare the query
        query = """
        query getJobExecutions($customRuleUuid: String,
                             $monitorUuid: String,
                             $historyDays: Int,
                             $cursor: String,
                             $first: Int) {
          getJobExecutions(customRuleUuid: $customRuleUuid,
                         monitorUuid: $monitorUuid,
                         historyDays: $historyDays,
                         after: $cursor,
                         first: $first) {
            pageInfo {
              endCursor
              hasNextPage
            }
            edges {
              node {
                jobExecutionUuid
                startTime
                endTime
                status
                exceptions
                exceptionsDetail {
                  type
                  description
                  sqlQuery
                }
              }
            }
          }
        }
        """
        
        # Prepare variables
        variables = {
            "historyDays": history_days,
            "first": first
        }
        
        if monitor_uuid:
            variables["monitorUuid"] = monitor_uuid
        
        if custom_rule_uuid:
            variables["customRuleUuid"] = custom_rule_uuid
        
        if cursor:
            variables["cursor"] = cursor
        
        # Execute the query
        result = self.mc_client.execute_query(query, variables)
        
        if hasattr(result, 'get_job_executions') and result.get_job_executions:
            executions = []
            
            if hasattr(result.get_job_executions, 'edges'):
                for edge in result.get_job_executions.edges:
                    if hasattr(edge, 'node'):
                        executions.append(deep_dict_convert(edge.node))
            
            # Add pagination info
            pagination = {}
            if hasattr(result.get_job_executions, 'page_info'):
                pagination = {
                    "endCursor": result.get_job_executions.page_info.end_cursor,
                    "hasNextPage": result.get_job_executions.page_info.has_next_page
                }
            
            return {
                "executions": executions,
                "pagination": pagination
            }
        
        LOGGER.error("Failed to get job executions")
        return {"executions": [], "pagination": {}}