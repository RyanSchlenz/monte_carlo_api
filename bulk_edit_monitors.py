#!/usr/bin/env python3
"""
bulk_edit_monitors.py - Tool for bulk editing Monte Carlo monitors
This script allows you to list all monitors, select specific ones by UUID, and update them.
Supports VALIDATION, CUSTOM_SQL, METRIC, STATS, and COMPARISON monitor types.
"""

import sys
import json
import argparse
import time
from typing import Dict, Any, List, Optional
from datetime import datetime

# Import from our modular components
from mc_client import MonteCarloClient, deep_dict_convert
from monitor_manager import MonitorManager
from monitor_types import (
    get_monitor_details, update_validation_monitor,
    update_comparison_monitor, update_stats_monitor
)
from monitor_utils import (
    list_monitors, select_monitors_by_uuid, 
    bulk_update_monitors, update_schedule_example,
    update_description_example, update_alert_thresholds_example,
    create_update_template, fill_template_interactively,
    apply_template_updates, get_graphql_schema
)

def main():
    parser = argparse.ArgumentParser(description='Bulk Edit Monte Carlo Monitors')
    parser.add_argument('--profile', help='Monte Carlo profile to use')
    parser.add_argument('--mcd-id', help='Monte Carlo ID (if not using profile)')
    parser.add_argument('--mcd-token', help='Monte Carlo Token (if not using profile)')
    parser.add_argument('--limit', type=int, default=100, help='Maximum number of monitors to list')
    parser.add_argument('--type', help='Filter by monitor type')
    parser.add_argument('--uuids', help='Comma-separated list of monitor UUIDs to update')
    parser.add_argument('--update-type', choices=['schedule', 'description', 'alerts', 'interactive'], 
                       default='interactive', help='Type of update to perform')
    parser.add_argument('--get-schema', action='store_true', help='Retrieve and print GraphQL schema')
    parser.add_argument('--template-file', help='JSON file with update template')
    
    args = parser.parse_args()
    
    # Create client
    if args.mcd_id and args.mcd_token:
        mc_client = MonteCarloClient(mcd_id=args.mcd_id, mcd_token=args.mcd_token)
    else:
        mc_client = MonteCarloClient(profile=args.profile)
    
    # Create manager
    manager = MonitorManager(mc_client)
    
    # List monitors
    print("\n=== LISTING MONITORS ===")
    monitors = list_monitors(manager, limit=args.limit, monitor_type=args.type)
    if not monitors:
        return

    if args.get_schema:
        get_graphql_schema(mc_client)
        return
    
    # Select monitors to update
    if args.uuids:
        uuids = [uuid.strip() for uuid in args.uuids.split(',')]
        selected_monitors = select_monitors_by_uuid(monitors, uuids)
    else:
        # If no UUIDs provided, ask user which monitors to update
        print("\nSelect monitors to update (comma-separated list of numbers, or 'all'):")
        selection = input("> ")
        
        if selection.lower() == 'all':
            selected_monitors = monitors
        else:
            try:
                indices = [int(idx.strip()) - 1 for idx in selection.split(',')]
                selected_monitors = [monitors[idx] for idx in indices if 0 <= idx < len(monitors)]
            except (ValueError, IndexError):
                print("Invalid selection. Exiting.")
                return
    
    if not selected_monitors:
        print("No monitors selected. Exiting.")
        return
    
    print(f"\nSelected {len(selected_monitors)} monitors for update.")
    
    # Load template from file if provided
    template_updates = None
    if args.template_file:
        try:
            with open(args.template_file, 'r') as f:
                template_updates = json.load(f)
            print(f"Loaded update template from {args.template_file}")
        except Exception as e:
            print(f"Error loading template file: {str(e)}")
            return
    
    # Determine which update function to use
    if args.update_type == 'schedule':
        update_fn = update_schedule_example
    elif args.update_type == 'description':
        update_fn = update_description_example
    elif args.update_type == 'alerts':
        update_fn = update_alert_thresholds_example
    elif args.update_type == 'interactive':
        # In interactive mode, we'll create a template for each monitor type
        # and ask the user to fill it in
        def interactive_update_fn(monitor):
            # Create template based on monitor type
            template = create_update_template(monitor.get('monitorType', 'VALIDATION'))
            
            # If template updates were loaded from a file, use those
            if template_updates:
                return apply_template_updates(monitor, template_updates)
            
            # Otherwise, fill the template interactively
            updates = fill_template_interactively(template)
            return apply_template_updates(monitor, updates)
        
        update_fn = interactive_update_fn
    else:
        print(f"Update type '{args.update_type}' not recognized. Exiting.")
        return
    
    # Perform bulk update
    print(f"\n=== PERFORMING BULK UPDATE: {args.update_type} ===")
    bulk_update_monitors(manager, selected_monitors, update_fn)

if __name__ == "__main__":
    main()