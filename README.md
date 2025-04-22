# Monte Carlo Monitor Manager

A modular Python library for managing Monte Carlo monitors and data quality rules via the GraphQL API.

## Overview

This package provides a modular way to work with Monte Carlo monitors, allowing you to:

- List and filter monitors
- Get detailed information about monitors
- Update monitor configurations in bulk
- Pause and unpause monitors
- Support for different monitor types:
  - VALIDATION
  - CUSTOM_SQL
  - METRIC
  - STATS
  - COMPARISON

## Module Structure

The package is organized in a modular way:

- `mc_client.py` - Base client for the Monte Carlo API
- `monitor_manager.py` - Core monitor management functionality
- `monitor_types.py` - Type-specific monitor functions
- `monitor_utils.py` - Utility functions for monitors
- `safe_graphql.py` - Safe GraphQL query utilities
- `bulk_edit_monitors.py` - Main script for bulk editing monitors

## Installation

### Prerequisites

This package requires Python 3.6 or higher.

### Dependencies

Install dependencies with:

```
pip install -r requirements.txt
```

Required dependencies:
- gql (for GraphQL API access)
- requests
- urllib3

Optional:
- pycarlo (provides additional functionality)

## Command Line Usage

The main script `bulk_edit_monitors.py` provides a command-line interface for bulk editing monitors:

```
usage: bulk_edit_monitors.py [-h] [--profile PROFILE] [--mcd-id MCD_ID]
                            [--mcd-token MCD_TOKEN] [--limit LIMIT]
                            [--type TYPE] [--uuids UUIDS]
                            [--update-type {schedule,description,alerts,interactive}]
                            [--get-schema] [--template-file TEMPLATE_FILE]

Bulk Edit Monte Carlo Monitors

optional arguments:
  -h, --help            show this help message and exit
  --profile PROFILE     Monte Carlo profile to use
  --mcd-id MCD_ID       Monte Carlo ID (if not using profile)
  --mcd-token MCD_TOKEN
                        Monte Carlo Token (if not using profile)
  --limit LIMIT         Maximum number of monitors to list
  --type TYPE           Filter by monitor type
  --uuids UUIDS         Comma-separated list of monitor UUIDs to update
  --update-type {schedule,description,alerts,interactive}
                        Type of update to perform
  --get-schema          Retrieve and print GraphQL schema
  --template-file TEMPLATE_FILE
                        JSON file with update template
```

## Examples

### List all monitors

```
python bulk_edit_monitors.py --profile my_profile
```

### Get GraphQL schema

```
python bulk_edit_monitors.py --profile my_profile --get-schema
```

### Update schedules for specific monitors

```
python bulk_edit_monitors.py --profile my_profile --uuids abc123,def456 --update-type schedule
```

### Interactive update mode

```
python bulk_edit_monitors.py --profile my_profile --update-type interactive
```

## Library Usage

You can also use the package as a library in your own Python code:

```python
from mc_client import MonteCarloClient
from monitor_manager import MonitorManager
from monitor_utils import list_monitors, bulk_update_monitors, update_schedule_example

# Create client and manager
mc_client = MonteCarloClient(profile="my_profile")
manager = MonitorManager(mc_client)

# List monitors
monitors = list_monitors(manager, limit=10)

# Apply updates
bulk_update_monitors(manager, monitors, update_schedule_example)
```

## Authentication

There are three ways to authenticate:

1. Using a profile from your `~/.mcd/profiles.ini` file
2. Directly providing MCD_ID and MCD_TOKEN
3. Using the default profile in your `~/.mcd/profiles.ini` file

## Monitor Update Templates

You can create JSON templates for monitor updates. Example:

```json
{
  "description": "Updated monitor description",
  "scheduleConfig": {
    "scheduleType": "FIXED",
    "intervalMinutes": 1440,
    "startTime": "2023-05-01T02:00:00.000Z"
  }
}
```

Save this to a file and use with `--template-file` option.

## Development

For development work, additional dependencies can be installed:

```
pip install pytest black pylint
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.