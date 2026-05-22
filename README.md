# ArcGIS Item Dependency Graph

A Python tool that comprehensively maps dependencies and relationships between all content items in an ArcGIS Enterprise Portal or ArcGIS Online organization. Results are exported to SQL Server for reporting and analysis.

## Features

- Supports both **ArcGIS Enterprise Portal** and **ArcGIS Online**
- **Incremental scan** mode — only processes items modified since last run
- **Multi-threaded** for performance (configurable thread pool)
- Exports to **SQL Server** with deduplication
- Captures datastore lineage, layer-level detail, and EGDB source info
- Email notifications on success/failure

## Discovery Methods

| Method | Description |
|--------|-------------|
| **Item Graph API** | Official Esri dependency traversal via `create_dependency_graph` |
| **Related Items API** | All known ArcGIS relationship types (forward & reverse) |
| **Data Scan** | Embedded item ID references found in JSON data blobs |
| **Operational Layer Parsing** | Web Maps, Dashboards, Experience Builder, StoryMaps |
| **Deep URL Scan** | Layer-level URL matching (e.g., `FeatureServer/3`) |
| **Service Sublayer Enumeration** | Internal structure of multi-layer services |
| **EGDB Source Discovery** | Enterprise geodatabase backing datasets via admin manifest |

## Requirements

- Python 3.x
- [ArcGIS API for Python](https://developers.arcgis.com/python/)
- SQL Server with ODBC Driver 17
- Required packages:
  ```
  arcgis
  pyodbc
  requests
  keyring
  ```

## Configuration

Edit the following variables in the script before running:

```python
# Email recipients for notifications
toEmails = ["your-email@example.com"]

# SQL Server connection
DB_SERVER   = r"YOUR_SQL_SERVER\INSTANCE"
DB_DATABASE = "YOUR_DATABASE"

# ArcGIS credentials (stored in OS keyring)
# For AGOL:
userName = "YOUR_AGOL_USERNAME"
keyring.set_password("arcgis_AGOL", userName, "your-password")

# For Portal:
userName = "YOUR_PORTAL_ADMIN_USERNAME"
keyring.set_password("arcgis_portal", userName, "your-password")

# SMTP email server
SERVER = "<your-email-server>"
FROM = "<your-email-address>"
```

### Credential Storage

Passwords are retrieved from the OS keyring at runtime — nothing is stored in the script. Use `keyring.set_password()` once to store your credentials before the first run.

## Usage

```bash
# Scan ArcGIS Enterprise Portal
python GitHubShare.py Portal

# Scan ArcGIS Online
python GitHubShare.py Agol
```

## How It Works

1. **Authenticates** to the target environment (Portal or AGOL)
2. **Fetches all items** in the organization
3. **Builds caches** — service URL index, datastore names, EGDB sources
4. **Scans each item** through all discovery methods (multi-threaded)
5. **Exports results** to SQL Server (full replace or incremental upsert)
6. **Logs & emails** a summary report

### Scan Modes

- **Full scan**: Processes all items and replaces the entire database table
- **Incremental scan**: Only processes items modified since the last run, deletes/re-inserts their records

The scan mode is determined automatically based on the log file history.

## Output

### SQL Server Table

Each record captures:
- **Origin item** metadata (ID, title, owner, type, URL, sharing level, dates)
- **Dependent item** metadata
- **Relationship** type, direction, and discovery method
- **Layer-level detail** (layer ID, name, URL) for service sublayers
- **Datastore name** (ArcGIS Server folder)
- **EGDB source datasets** (enterprise geodatabase feature classes/tables)

### Log File

A run summary is appended to `output/itemDependencies.txt` after each scan with statistics including items scanned, relationships found, and duration.

## Static Website (D3.js Visualizer)

In the `docs` folder is a static website that allows uploading the graph JSON file for visualization using D3.js.

- **Load Data** to select and load a local JSON file
- Nodes can be manually moved and locked in place (right-click to toggle)
- **Save as SVG** / **Save Graph** to export the current view
- **Enable Popups** for hover details (item ID, URL links)
- **Adjust physics options** to tune node positioning

## Author

**Bidemi Adejumo**  
Email: godofsix@gmail.com

## License

MIT

