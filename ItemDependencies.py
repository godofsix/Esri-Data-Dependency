import os, re, sys, json, keyring, logging, pyodbc, requests, smtplib
from urllib.parse import urlparse, urljoin
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from arcgis.gis import GIS
from arcgis.apps.itemgraph import create_dependency_graph


# ------------------ UTILITY FUNCTIONS ------------------

def logWrite(log_path, message):
    """Append a timestamped message to the log file."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def durationEnd(start_time):
    """Return a human-readable duration string from start_time to now."""
    elapsed = datetime.datetime.now() - start_time
    total_seconds = int(elapsed.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"


# Reusable HTTP session for connection pooling (TCP reuse)
_http_session = requests.Session()
_http_session.verify = False

# Max parallel threads for I/O-bound work
MAX_WORKERS = 12
# ------------------ CONFIGURATION ------------------
# Usage:  python DependenciesAllNewRoot.py [portal|agol]
#   portal  (default) — ArcGIS Enterprise Portal
#   agol               — ArcGIS Online

toEmails = ["your-email@example.com"]
scriptStartTime = datetime.datetime.now()

# ------------------ SQL Server config  ------------------
DB_SERVER   = r"YOUR_SQL_SERVER\INSTANCE"
DB_DATABASE = "YOUR_DATABASE"

OUTPUT_DIR = r"arcgis_item_graph-main\output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
LOG_FILE = os.path.join(OUTPUT_DIR, "itemDependencies.txt")

# These globals are set by Portal() or Agol() at runtime
target     = None
gis        = None
PORTAL_URL = None
DB_TABLE   = None

# ------------------ ALL ESRI ITEM TYPES ------------------
# Every known ArcGIS Portal/Online item type
ESRI_ITEM_TYPES = {
    # Maps
    "Web Map", "Web Scene", "CityEngine Web Scene", "Pro Map",

    # Layers & Services
    "Feature Service", "Map Service", "Image Service", "Scene Service",
    "Vector Tile Service", "KML", "KML Collection", "WMS", "WMTS",
    "WFS", "OGCFeatureServer", "Feature Collection",
    "Feature Collection Template", "Geodata Service", "Globe Service",
    "Network Analysis Service", "Geoprocessing Service",
    "Geometry Service", "Geocoding Service", "Stream Service",
    "Relational Database Connection",

    # Apps
    "Web Mapping Application", "Mobile Application", "Operation View",
    "Desktop Application", "Application",

    # Experience Builder / Dashboards / Instant Apps
    "Experience Builder Widget", "Dashboard", "Insights Page",
    "Insights Model", "Insights Theme",

    # StoryMaps
    "StoryMap", "Story Map",

    # AppBuilder / WAB
    "Web AppBuilder Widget",

    # Notebooks & Analytics
    "Notebook", "Big Data Analytic", "Real Time Analytic",
    "Excalibur Imagery Project", "Ortho Mapping Project",
    "Ortho Mapping Template", "Solution",

    # Data & Files
    "Shapefile", "File Geodatabase", "CSV", "Microsoft Excel",
    "GeoJson", "GeoPackage", "Statistical Data Collection",
    "Document Link", "Microsoft Word", "Microsoft Powerpoint",
    "PDF", "Image", "Visio Document", "iWork Keynote",
    "iWork Pages", "iWork Numbers", "CAD Drawing", "Report Template",

    # Tools & Forms
    "Geoprocessing Package", "Locator Package", "Map Package",
    "Tile Package", "Vector Tile Package", "Scene Package",
    "Layer Package", "Explorer Map", "Globe Document",
    "Scene Document", "Published Map", "Map Template",
    "Windows Mobile Package", "Layout", "Project Package",
    "Project Template", "Survey123 Add In", "Raster function template",
    "Rules Package", "Symbol Set", "Color Set",
    "Workflow Manager Package", "Desktop Style",
    "Form", "Survey", "Hub Initiative", "Hub Site Application",
    "Hub Page", "Hub Event", "Hub Initiative Template",
    "Urban Model", "Workforce Project",

    # Content & Config
    "Style", "Administrative Report", "Storymap Theme",
    "Deep Learning Package", "Deep Learning Studio Project",
    "Mission", "Mission Report",
}

# All known ArcGIS relationship types (forward + reverse)
RELATIONSHIP_TYPES = [
    "Map-Service", "WMA-Code", "Map-FeatureCollection",
    "MobileApp-Code", "Service-Data", "Service-Style",
    "Survey-Service", "Survey-Data", "Service-Layer",
    "Map-Area", "Service-Route", "Solution-Item",
    "APIKey-Item", "Mission-Item", "Map-AppConfig",
    "App-Data", "ExperienceBuilder-Data",
    "StoryMap-Data", "Dashboard-Data",
    "InsightsWorkbook-Item", "Notebook-Data",
    "WorkforceMap-FeatureService", "WorkforceProject-DispatcherMap",
    "WorkforceProject-WorkerMap", "WorkforceProject-ProjectItem",
    "WorkforceProject-FeatureService",
    "HubInitiative-StakeholderOrg", "HubInitiative-Event",
    "HubInitiative-InformApp", "HubInitiative-FollowApp",
    "HubSite-Theme", "HubSite-Initiative", "HubSite-Page",
    "TrackedMap-Locations",
]

# ------------------ INCREMENTAL RUN SUPPORT ------------------

def get_last_run_timestamp(log_path, environment):
    """
    Parse the log file and return the most recent 'End Time' for the
    given environment (portal/agol) as a UTC-aware datetime.
    Returns None if the file doesn't exist, is empty, or contains no
    matching entries for that environment.
    """
    if not os.path.isfile(log_path):
        return None
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return None

    # Split the log into individual run blocks (separated by '===')
    # and walk them in reverse to find the latest matching environment.
    blocks = re.split(r'(?=={3,}\n\s*ArcGIS Item Dependency Scan)', content)

    for block in reversed(blocks):
        env_match = re.search(r'Environment\s*:\s*(\S+)', block, re.IGNORECASE)
        if not env_match:
            continue
        if env_match.group(1).strip().lower() != environment.lower():
            continue
        end_match = re.search(
            r'End Time\s*:\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', block
        )
        if end_match:
            return datetime.datetime.strptime(
                end_match.group(1), '%Y-%m-%d %H:%M:%S'
            ).replace(tzinfo=datetime.timezone.utc)

    return None

# ------------------ HELPERS ------------------

def format_date(epoch_ms):
    if epoch_ms:
        return datetime.datetime.fromtimestamp(epoch_ms / 1000, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return None

def get_sharing_level(item):
    try:
        mapping = {
            'public':  'Public',
            'org':     'Organization',
            'shared':  'Shared (Groups)',
            'private': 'Private'
        }
        return mapping.get(item.access, 'Unknown')
    except:
        return 'Unknown'

def get_item_url(item_id):
    return f"{PORTAL_URL}/home/item.html?id={item_id}"

def safe_get(item, attr, default=None):
    try:
        return getattr(item, attr, default)
    except:
        return default

def build_item_record(item):
    """Build a standardized dict of metadata for any portal item."""
    return {
        "id":       item.id,
        "title":    safe_get(item, "title", "Unknown"),
        "owner":    safe_get(item, "owner", "Unknown"),
        "type":     safe_get(item, "type",  "Unknown"),
        "url":      get_item_url(item.id),
        "serviceUrl": safe_get(item, "url"),  # The actual REST service endpoint
        "sharing":  get_sharing_level(item),
        "created":  format_date(safe_get(item, "created")),
        "modified": format_date(safe_get(item, "modified")),
    }

# ----------- SERVICE TYPES THAT CAN HAVE SUBLAYERS -----------
LAYERED_SERVICE_TYPES = {
    "Feature Service", "Map Service", "Image Service",
    "Scene Service", "Vector Tile Service",
}


def get_service_layers(item):
    """
    Query the REST endpoint of a service item to enumerate all its layers
    and tables.  Returns a list of dicts:
        [{"id": 0, "name": "Parcels", "type": "Feature Layer",
          "url": "https://...FeatureServer/0", "geometryType": "esriGeometryPolygon"}, ...]
    """
    layers = []
    service_url = safe_get(item, "url")
    if not service_url:
        return layers

    try:
        # Query the service root with ?f=json
        resp = _http_session.get(
            service_url.rstrip('/'),
            params={"f": "json"},
            timeout=30,
        )
        resp.raise_for_status()
        svc_json = resp.json()

        # Collect layers + tables
        for section_key in ("layers", "tables"):
            for lyr in svc_json.get(section_key, []):
                lyr_id   = lyr.get("id")
                lyr_name = lyr.get("name", f"Layer {lyr_id}")
                lyr_type = lyr.get("type", section_key.rstrip('s').title())
                lyr_geom = lyr.get("geometryType", "")
                lyr_url  = f"{service_url.rstrip('/')}/{lyr_id}"

                layers.append({
                    "id":           lyr_id,
                    "name":         lyr_name,
                    "type":         lyr_type,
                    "url":          lyr_url,
                    "geometryType": lyr_geom,
                })

                # Recurse into sublayers if present (grouped layers)
                for sub in lyr.get("subLayerIds", []) or []:
                    sub_url = f"{service_url.rstrip('/')}/{sub}"
                    layers.append({
                        "id":   sub,
                        "name": f"{lyr_name}/sublayer-{sub}",
                        "type": "Sub Layer",
                        "url":  sub_url,
                        "geometryType": "",
                    })

    except Exception as e:
        print(f" Could not enumerate layers for {item.title}: {e}")

    return layers


# Module-level cache: item_id → list of layer dicts from get_service_layers()
_service_layers_detail_cache = {}


def _fetch_layers_for_item(item):
    """Fetch layers for a single service item (used by thread pool)."""
    svc_layers = get_service_layers(item)
    return item, svc_layers


def build_service_url_index(all_items):
    """
    Build a reverse-lookup dict:
        normalized_url  →  (item, layer_info_or_None)

    This maps every known service URL (with and without layer indices)
    back to the owning portal item.  Used by the deep URL scanner to
    resolve layer-level references found inside web maps / apps.

    Also populates _service_layers_detail_cache and _service_layers_cache
    so that later discovery methods don't re-fetch layers.
    """
    url_index = {}   # url_lower → (item, layer_dict | None)

    # Separate service items that need layer enumeration
    service_items = []
    for item in all_items:
        svc_url = safe_get(item, "url")
        if not svc_url:
            continue
        base = svc_url.rstrip('/').lower()
        url_index[base] = (item, None)
        if item.type in LAYERED_SERVICE_TYPES:
            service_items.append(item)

    # Fetch layers in parallel
    print(f"  Enumerating layers for {len(service_items)} service(s) with {MAX_WORKERS} threads...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_layers_for_item, item): item for item in service_items}
        for future in as_completed(futures):
            try:
                item, svc_layers = future.result()
            except Exception:
                continue
            _service_layers_detail_cache[item.id] = svc_layers
            if svc_layers:
                _service_layers_cache[item.id] = ", ".join(
                    lyr["name"] for lyr in svc_layers if lyr.get("name")
                )
            for lyr in svc_layers:
                lyr_url = lyr["url"].rstrip('/').lower()
                url_index[lyr_url] = (item, lyr)

    print(f"  Service URL index built — {len(url_index)} entries.")
    print(f"  Service layer names cached for {len(_service_layers_cache)} service(s).")
    return url_index


def _normalize_url(url):
    """Strip query params, trailing slash, lowercase for comparison."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/').lower()


# ----------- DATASTORE DISCOVERY -----------

# Module-level cache populated during initialization
_datastore_cache = {}

# Module-level cache: item_id → comma-separated layer names string
_service_layers_cache = {}

# Module-level cache: item_id → list of EGDB dataset dicts from admin manifest
_egdb_source_cache = {}


def extract_datastore_from_url(service_url):
    """
    Extract the datastore (folder) name from a service URL.
    URL pattern: .../rest/services/{DatastoreName}/{ServiceName}/{ServiceType}
    Returns None if the URL doesn't match the pattern (e.g. web apps).
    """ 
    if not service_url:
        return None
    match = re.search(r'/rest/services/([^/]+)/[^/]+/(?:Feature|Map|Image|Scene|Vector|WFS|WMS|Geoprocessing|Geocode|Geometry|Network|Stream|Globe|Geodata)Server', service_url, re.IGNORECASE)
    if match:
        return match.group(1) 
    return None


def get_service_datastore_info(item):
    """
    For a service item, determine the underlying datastore name.
    Primary source: parse the service URL for the folder name after /rest/services/.
    e.g. .../rest/services/EnterpriseLibrary/TransitStops/FeatureServer → 'EnterpriseLibrary'
    Fallback: 'Service-Data' related item (shown as 'Created from' in Portal UI).
    Items without a service URL (web apps, etc.) will have no datastore name.
    """
    info = {"datastore_name": None}

    # --- Approach 1: Extract folder name from the service URL ---
    service_url = safe_get(item, "url")
    ds_from_url = extract_datastore_from_url(service_url)
    if ds_from_url:
        info["datastore_name"] = ds_from_url
        return info

    # --- Approach 2: Related Data Store item ("Created from" in Portal UI) ---
    for direction in ["forward", "reverse"]:
        try:
            related = item.related_items("Service-Data", direction)
            for rel_item in (related or []):
                if not rel_item.title:
                    continue
                rel_type = safe_get(rel_item, "type", "")
                if rel_type == "Data Store":
                    info["datastore_name"] = rel_item.title
                    return info
        except Exception:
            pass

    return info

def build_datastore_cache(all_items):
    """Pre-compute datastore name for all items, using threads for items that need API calls."""
    cache = {}
    need_api = []  # items where URL parsing alone can't determine datastore

    # First pass: resolve via URL parsing (instant, no API call)
    for item in all_items:
        service_url = safe_get(item, "url")
        ds_from_url = extract_datastore_from_url(service_url)
        if ds_from_url:
            cache[item.id] = {"datastore_name": ds_from_url}
        else:
            need_api.append(item)

    print(f"  Datastore: {len(cache)} resolved from URL, {len(need_api)} need API lookup...")

    # Second pass: use thread pool for items needing related_items API
    def _fetch_ds(item):
        return item.id, get_service_datastore_info(item)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_ds, item): item for item in need_api}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 200 == 0:
                print(f"    ... {done}/{len(need_api)}")
            try:
                item_id, info = future.result()
                cache[item_id] = info
            except Exception:
                cache[futures[future].id] = {"datastore_name": None}

    populated = sum(1 for v in cache.values() if v["datastore_name"])
    print(f"  Datastore cache built — {len(cache)} items, {populated} with datastore name.")
    return cache


# ----------- EGDB SOURCE DISCOVERY (Admin Manifest) -----------

def _get_admin_token():
    """Get auth token for ArcGIS Server admin API calls."""
    try:
        return gis._con.token
    except:
        return None


def _build_manifest_url(service_url):
    """
    Convert a service REST URL to its admin manifest URL.
    e.g. .../rest/services/Folder/Name/FeatureServer
      → .../admin/services/Folder/Name.FeatureServer/iteminfo/manifest/manifest.json
    """
    if not service_url:
        return None
    match = re.match(
        r'(.*?)/rest/services/(.+)/(FeatureServer|MapServer|ImageServer|'
        r'SceneServer|VectorTileServer)',
        service_url, re.IGNORECASE
    )
    if not match:
        return None
    base  = match.group(1)  
    path  = match.group(2)   
    stype = match.group(3)   
    return f"{base}/admin/services/{path}.{stype}/iteminfo/manifest/manifest.json"


def fetch_service_manifest(service_url):
    """
    Fetch the service manifest from ArcGIS Server admin API.
    Returns list of dicts: [{"datasetName": "...", "connectionString": "..."}, ...]
    """
    manifest_url = _build_manifest_url(service_url)
    if not manifest_url:
        return []

    token = _get_admin_token()
    try:
        params = {"f": "json"}
        if token:
            params["token"] = token

        resp = _http_session.get(
            manifest_url, params=params,
            timeout=30,
        )
        resp.raise_for_status()
        manifest = resp.json()

        # Check for error response
        if "error" in manifest or "status" in manifest:
            return []

        datasets = []
        for db in manifest.get("databases", []):
            conn_str = db.get("onServerConnectionString", "")
            for ds in db.get("datasets", []):
                ds_name = ds.get("onServerName", "")
                if ds_name:
                    datasets.append({
                        "datasetName": ds_name,
                        "connectionString": conn_str,
                    })
        return datasets

    except Exception:
        return []


def build_egdb_source_cache(all_items):
    """Pre-compute EGDB source datasets for all service items via admin manifest (threaded)."""
    cache = {}
    service_items = [i for i in all_items if i.type in LAYERED_SERVICE_TYPES]
    print(f"  Querying EGDB sources (admin manifest) for {len(service_items)} service(s) with {MAX_WORKERS} threads...")

    token = _get_admin_token()
    if not token:
        print("  WARNING: No admin token available — EGDB source discovery may be limited.")

    def _fetch_manifest(item):
        service_url = safe_get(item, "url")
        if not service_url:
            return item.id, []
        return item.id, fetch_service_manifest(service_url)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_manifest, item): item for item in service_items}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f"    ... {done}/{len(service_items)}")
            try:
                item_id, datasets = future.result()
                if datasets:
                    cache[item_id] = datasets
            except Exception:
                pass

    populated = sum(1 for v in cache.values() if v)
    print(f"  EGDB source cache built — {populated} service(s) with EGDB source info.")
    return cache


# ------------------ DB SETUP ------------------

def get_db_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_DATABASE};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def create_table_if_not_exists(conn):
    """Create the dependencies table if it doesn't already exist."""
    cursor = conn.cursor()
    cursor.execute(f"""
        IF NOT EXISTS (
            SELECT * FROM sysobjects
            WHERE name='{DB_TABLE.split('.')[-1]}' AND xtype='U'
        )
        CREATE TABLE {DB_TABLE} (
            ID                      INT IDENTITY(1,1) PRIMARY KEY,
            RunTimestamp            DATETIME          NOT NULL,

            -- Origin (the item that references or owns the dependency)
            OriginItemID            NVARCHAR(64)      NOT NULL,
            OriginItemTitle         NVARCHAR(512),
            OriginItemOwner         NVARCHAR(256),
            OriginItemType          NVARCHAR(256),
            OriginItemURL           NVARCHAR(1024),
            OriginServiceURL        NVARCHAR(1024),
            OriginItemSharing       NVARCHAR(64),
            OriginItemCreated       DATETIME,
            OriginItemModified      DATETIME,

            -- Dependent (the item being referenced)
            DependentItemID         NVARCHAR(64)      NULL,
            DependentItemTitle      NVARCHAR(512),
            DependentItemOwner      NVARCHAR(256),
            DependentItemType       NVARCHAR(256),
            DependentItemURL        NVARCHAR(1024),
            DependentServiceURL     NVARCHAR(1024),
            DependentItemSharing    NVARCHAR(64),
            DependentItemCreated    DATETIME,
            DependentItemModified   DATETIME,
            HasRelationship         BIT               NOT NULL, -- True if real relationship, False if placeholder for no-relationship

            -- Relationship metadata
            RelationshipType        NVARCHAR(256),
            RelationshipDirection   NVARCHAR(16),     -- 'forward' or 'reverse'
            DiscoveryMethod         NVARCHAR(64),     -- how the link was found
            
            -- Layer-level detail for services with sublayers
            DependentLayerID        INT               NULL,
            DependentLayerName      NVARCHAR(512)     NULL,
            DependentLayerURL       NVARCHAR(1024)    NULL,

            -- Datastore
            DatastoreName           NVARCHAR(512)     NULL,

            -- All layer names for the service
            ServiceLayerNames       NVARCHAR(MAX)     NULL,

            -- EGDB source feature classes / tables (from admin manifest)
            SourceEGDBDatasets      NVARCHAR(MAX)     NULL,

            -- Dedup constraint: skip if same pair already exists
            CONSTRAINT UQ_{DB_TABLE.split('.')[-1]}_OriginDependent UNIQUE (
                OriginItemID, DependentItemID, RelationshipType, RelationshipDirection
            )
        )
    """)
    conn.commit()
    print(f"Table {DB_TABLE} ready.")

def insert_record(cursor, record):
    """Insert a single relationship record, skip if duplicate (append-only)."""
    sql = f"""
        INSERT INTO {DB_TABLE} (
            RunTimestamp,
            OriginItemID, OriginItemTitle, OriginItemOwner,
            OriginItemType, OriginItemURL, OriginServiceURL, OriginItemSharing,
            OriginItemCreated, OriginItemModified,
            DependentItemID, DependentItemTitle, DependentItemOwner,
            DependentItemType, DependentItemURL, DependentServiceURL, DependentItemSharing,
            DependentItemCreated, DependentItemModified,
            RelationshipType, RelationshipDirection, DiscoveryMethod, HasRelationship,
            DependentLayerID, DependentLayerName, DependentLayerURL,
            DatastoreName,
            ServiceLayerNames,
            SourceEGDBDatasets
        )
        SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        WHERE NOT EXISTS (
            SELECT 1 FROM {DB_TABLE}
            WHERE OriginItemID        = ?
              AND DependentItemID     = ?
              AND RelationshipType    = ?
              AND RelationshipDirection = ?
        )
    """
    cursor.execute(sql, (
        record["run_timestamp"],
        record["origin_id"],    record["origin_title"],   record["origin_owner"],
        record["origin_type"],  record["origin_url"],     record["origin_serviceUrl"],  record["origin_sharing"],
        record["origin_created"],  record["origin_modified"],
        record["dep_id"],       record["dep_title"],      record["dep_owner"],
        record["dep_type"],     record["dep_url"],        record["dep_serviceUrl"],     record["dep_sharing"],
        record["dep_created"],  record["dep_modified"],
        record["rel_type"],     record["rel_direction"],  record["discovery_method"],
        record["has_relationship"],
        record.get("dep_layer_id"),  record.get("dep_layer_name"),  record.get("dep_layer_url"),
        record.get("datastore_name"),
        record.get("service_layer_names"),
        record.get("source_egdb_datasets"),
        # WHERE NOT EXISTS params
        record["origin_id"],    record["dep_id"],
        record["rel_type"],     record["rel_direction"],
    ))

# ------------------ DISCOVERY ------------------

def discover_via_itemgraph(item, item_lookup, origin_meta, found_pairs):
    """Use create_dependency_graph for official Esri relationship traversal."""
    records = []
    try:
        graph = create_dependency_graph(item, gis=gis)

        for edge in graph.edges:
            src_id = edge.get("source") or edge.get("from")
            tgt_id = edge.get("target") or edge.get("to")
            rel     = edge.get("relationship", "itemgraph_edge")

            if not src_id or not tgt_id:
                continue

            # Forward: origin → dependent
            if src_id == item.id and tgt_id in item_lookup:
                pair = (item.id, tgt_id, rel, "forward")
                if pair not in found_pairs:
                    found_pairs.add(pair)
                    dep_meta = build_item_record(item_lookup[tgt_id])
                    records.append(build_record(
                        origin_meta, dep_meta, rel, "forward", "itemgraph"
                    ))

            # Reverse: something else → this item
            if tgt_id == item.id and src_id in item_lookup:
                pair = (src_id, item.id, rel, "reverse")
                if pair not in found_pairs:
                    found_pairs.add(pair)
                    dep_meta = build_item_record(item_lookup[src_id])
                    records.append(build_record(
                        origin_meta, dep_meta, rel, "reverse", "itemgraph"
                    ))

    except Exception as e:
        pass  # itemgraph may not support all types; fall through to other methods

    return records


def discover_via_related_items(item, item_lookup, origin_meta, found_pairs):
    """Query all known ArcGIS relationship types in both directions."""
    records = []
    for rel_type in RELATIONSHIP_TYPES:
        for direction in ["forward", "reverse"]:
            try:
                related = item.related_items(rel_type, direction)
                for dep in related:
                    if dep.id not in item_lookup or dep.id == item.id:
                        continue
                    pair = (item.id, dep.id, rel_type, direction)
                    if pair not in found_pairs:
                        found_pairs.add(pair)
                        dep_meta = build_item_record(dep)
                        records.append(build_record(
                            origin_meta, dep_meta, rel_type, direction, "related_items_api"
                        ))
            except:
                pass
    return records


def discover_via_data_scan(item, item_lookup, origin_meta, found_pairs, item_data=None):
    """Scan item JSON data blob for embedded item IDs."""
    records = []
    try:
        data = item_data if item_data is not None else item.get_data()
        if not data:
            return records
        data_str = json.dumps(data) if isinstance(data, dict) else str(data)

        for target_id, target_item in item_lookup.items():
            if target_id == item.id:
                continue
            if target_id in data_str:
                pair = (item.id, target_id, "Embedded_Reference", "forward")
                if pair not in found_pairs:
                    found_pairs.add(pair)
                    dep_meta = build_item_record(target_item)
                    records.append(build_record(
                        origin_meta, dep_meta,
                        "Embedded_Reference", "forward", "data_scan"
                    ))
    except:
        pass
    return records


def discover_via_operational_layers(item, item_lookup, origin_meta, found_pairs, item_data=None):
    """
    For Web Maps, Web Scenes, Dashboards, Experience Builder,
    StoryMaps, Instant Apps — parse their JSON for layer/widget references.
    """
    records = []
    try:
        data = item_data if item_data is not None else item.get_data()
        if not data:
            return records

        data_str = json.dumps(data) if isinstance(data, dict) else str(data)

        # ── Web Map / Web Scene operational layers ──
        for section in ["operationalLayers", "tables", "baseMap", "ground"]:
            layers = []
            if isinstance(data, dict):
                val = data.get(section, [])
                if isinstance(val, dict):
                    layers = val.get("baseMapLayers", [])
                elif isinstance(val, list):
                    layers = val

            for layer in layers:
                _extract_layer_ref(layer, item, item_lookup, origin_meta,
                                   found_pairs, records, "webmap_layer")

        # ── Dashboard widgets / data sources ──
        if isinstance(data, dict):
            for widget in data.get("widgets", []):
                ds = widget.get("dataSource", {})
                item_id = ds.get("itemId") or ds.get("id")
                if item_id and item_id in item_lookup and item_id != item.id:
                    _add_layer_record(item_id, item, item_lookup, origin_meta,
                                      found_pairs, records, "dashboard_datasource")

        # ── Experience Builder / StoryMap / Instant App ──
        # These embed item IDs in various nested structures — scan all string UUIDs
        uuid_pattern = re.compile(r'[0-9a-f]{32}')
        found_ids = set(uuid_pattern.findall(data_str))
        for fid in found_ids:
            if fid != item.id and fid in item_lookup:
                pair = (item.id, fid, "app_content_reference", "forward")
                if pair not in found_pairs:
                    found_pairs.add(pair)
                    dep_meta = build_item_record(item_lookup[fid])
                    records.append(build_record(
                        origin_meta, dep_meta,
                        "app_content_reference", "forward",
                        "operational_layer_scan"
                    ))

    except Exception as e:
        pass

    return records


def _extract_layer_ref(layer, item, item_lookup, origin_meta,
                       found_pairs, records, rel_label):
    if not isinstance(layer, dict):
        return
    item_id  = layer.get("itemId") or layer.get("id")
    layer_url = layer.get("url", "")

    if item_id and item_id in item_lookup and item_id != item.id:
        _add_layer_record(item_id, item, item_lookup, origin_meta,
                          found_pairs, records, rel_label)
    elif layer_url:
        for tid, titem in item_lookup.items():
            if tid == item.id:
                continue
            try:
                if titem.url and titem.url.rstrip('/') in layer_url:
                    _add_layer_record(tid, item, item_lookup, origin_meta,
                                      found_pairs, records, f"{rel_label}_url")
                    break
            except:
                pass

    # Recurse into sublayers
    for sub in layer.get("layers", []) + layer.get("sublayers", []):
        _extract_layer_ref(sub, item, item_lookup, origin_meta,
                           found_pairs, records, rel_label)


def _add_layer_record(dep_id, item, item_lookup, origin_meta,
                      found_pairs, records, rel_label,
                      layer_id=None, layer_name=None, layer_url=None):
    pair_key = f"{rel_label}__L{layer_id}" if layer_id is not None else rel_label
    pair = (item.id, dep_id, pair_key, "forward")
    if pair not in found_pairs:
        found_pairs.add(pair)
        dep_meta = build_item_record(item_lookup[dep_id])
        records.append(build_record(
            origin_meta, dep_meta, rel_label, "forward", "operational_layer_scan",
            layer_id=layer_id, layer_name=layer_name, layer_url=layer_url,
        ))


def build_record(origin_meta, dep_meta, rel_type, direction, method,
                 layer_id=None, layer_name=None, layer_url=None):
    # Pick datastore name from whichever side is a service (prefer dependent)
    dep_ds    = _datastore_cache.get(dep_meta["id"], {})
    origin_ds = _datastore_cache.get(origin_meta["id"], {})
    ds_name = dep_ds.get("datastore_name") or origin_ds.get("datastore_name")

    return {
        "run_timestamp":   datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "origin_id":       origin_meta["id"],
        "origin_title":    origin_meta["title"],
        "origin_owner":    origin_meta["owner"],
        "origin_type":     origin_meta["type"],
        "origin_url":      origin_meta["url"],
        "origin_serviceUrl": origin_meta["serviceUrl"],
        "origin_sharing":  origin_meta["sharing"],
        "origin_created":  origin_meta["created"],
        "origin_modified": origin_meta["modified"],
        "dep_id":          dep_meta["id"],
        "dep_title":       dep_meta["title"],
        "dep_owner":       dep_meta["owner"],
        "dep_type":        dep_meta["type"],
        "dep_url":         dep_meta["url"],
        "dep_serviceUrl":  dep_meta["serviceUrl"],
        "dep_sharing":     dep_meta["sharing"],
        "dep_created":     dep_meta["created"],
        "dep_modified":    dep_meta["modified"],
        "has_relationship": True,
        "rel_type":        rel_type,
        "rel_direction":   direction,
        "discovery_method": method,
        # Layer-level detail (populated for services with sublayers)
        "dep_layer_id":    layer_id,      # e.g. 0, 1, 2, 3, 4
        "dep_layer_name":  layer_name,    # e.g. "Waterways", "Roads"
        "dep_layer_url":   layer_url,     # e.g. ".../FeatureServer/2"
        # Datastore name (from whichever side is the service)
        "datastore_name":  ds_name,
        # Layer name: use the specific layer name if provided, otherwise fall back to full service layer list
        "service_layer_names": layer_name if layer_name else (_service_layers_cache.get(dep_meta["id"]) or _service_layers_cache.get(origin_meta["id"])),
        # EGDB source datasets (from admin manifest) — prefer dependent side
        "source_egdb_datasets": _format_egdb_datasets(dep_meta["id"]) or _format_egdb_datasets(origin_meta["id"]),
    }


def _format_egdb_datasets(item_id):
    """Return comma-separated EGDB dataset names for a service item, or None."""
    datasets = _egdb_source_cache.get(item_id, [])
    if not datasets:
        return None
    return ", ".join(ds["datasetName"] for ds in datasets if ds.get("datasetName"))


# --------------- NEW: SERVICE SUBLAYER DISCOVERY ---------------

def discover_service_sublayers(item, origin_meta, found_pairs):
    """
    For Feature Services / Map Services / etc., enumerate every layer
    and record each one as a "Service_SubLayer" relationship.
    This captures the internal structure of complex services.
    Uses cached layer data from the URL index build phase.
    """
    records = []
    if item.type not in LAYERED_SERVICE_TYPES:
        return records

    svc_layers = _service_layers_detail_cache.get(item.id) or []
    if not svc_layers:
        return records

    print(f"├─ Service has {len(svc_layers)} layer(s)/table(s)")

    for lyr in svc_layers:
        pair = (item.id, item.id, f"Service_SubLayer__{lyr['id']}", "self")
        if pair not in found_pairs:
            found_pairs.add(pair)
            # The dependent is the service itself (self-reference with layer detail)
            dep_meta = build_item_record(item)
            records.append(build_record(
                origin_meta, dep_meta,
                "Service_SubLayer", "self", "service_layer_enum",
                layer_id=lyr["id"],
                layer_name=lyr["name"],
                layer_url=lyr["url"],
            ))
    return records


# --------------- NEW: EGDB SOURCE RELATIONSHIP ---------------

def discover_egdb_sources(item, origin_meta, found_pairs):
    """
    For services backed by EGDB (enterprise geodatabase), create a
    'Service_EGDB_Source' relationship record for each backing
    feature class or table found in the admin manifest.
    This is the connection from the web service to its referenced
    egdb feature class or table.
    """
    records = []
    if item.type not in LAYERED_SERVICE_TYPES:
        return records

    datasets = _egdb_source_cache.get(item.id, [])
    if not datasets:
        return records

    print(f"├─ Service has {len(datasets)} EGDB source dataset(s)")

    for ds in datasets:
        ds_name = ds.get("datasetName", "")
        if not ds_name:
            continue
        pair = (item.id, item.id, f"Service_EGDB_Source__{ds_name}", "self")
        if pair not in found_pairs:
            found_pairs.add(pair)
            dep_meta = build_item_record(item)
            rec = build_record(
                origin_meta, dep_meta,
                "Service_EGDB_Source", "self", "admin_manifest",
                layer_name=ds_name,
            )
            # Override the source_egdb_datasets to just this single dataset
            # (full list is already on the record from build_record)
            records.append(rec)
    return records


# ------------ NEW: DEEP URL / LAYER-LEVEL SCAN -----------------

def discover_via_deep_url_scan(item, item_lookup, origin_meta,
                               found_pairs, service_url_index, item_data=None):
    """
    For Web Maps, Dashboards, Experience Builder, StoryMaps, and any
    app that embeds service URLs — do a layer-level URL match:

    1. Extract every URL from the item's JSON data.
    2. Look up each URL in the service_url_index (which includes
       individual layer endpoints like .../FeatureServer/3).
    3. Record the relationship with full layer detail.

    This catches references that the simple item-ID scan misses,
    especially when a web map points at layer 2 of a 5-layer service.
    """
    records = []
    try:
        data = item_data if item_data is not None else item.get_data()
        if not data:
            return records

        data_str = json.dumps(data) if isinstance(data, dict) else str(data)

        # Extract all URLs from the JSON blob
        url_pattern = re.compile(
            r'https?://[^\s"\',\]\}]+(?:/(?:FeatureServer|MapServer|ImageServer|'
            r'SceneServer|VectorTileServer|WFSServer|WMSServer)(?:/\d+)?)',
            re.IGNORECASE
        )
        found_urls = set(url_pattern.findall(data_str))

        for url in found_urls:
            norm = _normalize_url(url)
            match = service_url_index.get(norm)
            if not match:
                # Try stripping the layer index to match the base service
                base_norm = re.sub(r'/\d+$', '', norm)
                match = service_url_index.get(base_norm)
                if match:
                    # We matched the base service; extract layer index from URL
                    lyr_idx_match = re.search(r'/(\d+)$', norm)
                    lyr_idx = int(lyr_idx_match.group(1)) if lyr_idx_match else None
                    matched_item, _ = match
                    if matched_item.id == item.id:
                        continue
                    pair = (item.id, matched_item.id,
                            f"deep_url_layer__{lyr_idx}", "forward")
                    if pair not in found_pairs:
                        found_pairs.add(pair)
                        dep_meta = build_item_record(matched_item)
                        records.append(build_record(
                            origin_meta, dep_meta,
                            "deep_url_layer_ref", "forward",
                            "deep_url_scan",
                            layer_id=lyr_idx,
                            layer_name=None,
                            layer_url=url,
                        ))
                continue

            matched_item, layer_info = match
            if matched_item.id == item.id:
                continue

            lyr_id   = layer_info["id"]   if layer_info else None
            lyr_name = layer_info["name"] if layer_info else None

            pair = (item.id, matched_item.id,
                    f"deep_url_layer__{lyr_id}", "forward")
            if pair not in found_pairs:
                found_pairs.add(pair)
                dep_meta = build_item_record(matched_item)
                records.append(build_record(
                    origin_meta, dep_meta,
                    "deep_url_layer_ref", "forward",
                    "deep_url_scan",
                    layer_id=lyr_id,
                    layer_name=lyr_name,
                    layer_url=url,
                ))

    except Exception as e:
        print(f"Deep URL scan error for {item.title}: {e}")

    return records


# ------------------ MAIN DISCOVERY LOOP ------------------

def discover_all_dependencies(last_run_ts=None):
    start_time = datetime.datetime.now()
    print("Fetching all portal items...")
    all_items = gis.content.search(query="*", max_items=10000)
    item_lookup = {item.id: item for item in all_items}
    print(f"Total items fetched: {len(all_items)}\n")

    # ---- Determine which items to scan (incremental vs full) ----
    if last_run_ts:
        last_run_epoch_ms = last_run_ts.timestamp() * 1000
        items_to_scan = [
            i for i in all_items
            if (safe_get(i, "modified") or 0) > last_run_epoch_ms
               or (safe_get(i, "created") or 0) > last_run_epoch_ms
        ]
        print(f"Incremental mode: {len(items_to_scan)} item(s) changed since {last_run_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    else:
        items_to_scan = list(all_items)
        print("Full scan mode: processing all items")
    print()

    # Build service URL index for deep layer-level matching
    print("Building service URL index (enumerating layers for all services)...")
    service_url_index = build_service_url_index(all_items)
    print()

    # Build datastore cache for all service items
    global _datastore_cache
    print("Building datastore cache (querying service endpoints)...")
    _datastore_cache = build_datastore_cache(all_items)
    print()

    # Build EGDB source cache for all service items (admin manifest)
    global _egdb_source_cache
    print("Building EGDB source cache (querying admin manifests)...")
    _egdb_source_cache = build_egdb_source_cache(all_items)
    print()

    all_records = []
    changed_item_ids = set(i.id for i in items_to_scan)
    items_scanned = 0
    items_with_relationships = 0
    items_without_relationships = 0
    total_relationships = 0

    # Filter out Esri system accounts before threading
    items_to_process = [
        item for item in items_to_scan
        if not (item.owner and "esri_" in item.owner.lower())
    ]
    skipped_esri = len(items_to_scan) - len(items_to_process)
    if skipped_esri:
        print(f"Skipped {skipped_esri} Esri system-owned item(s).\n")

    def _process_single_item(item):
        """Process one item through all discovery methods. Thread-safe."""
        origin_meta = build_item_record(item)
        found_pairs = set()
        item_records = []

        # Fetch item data ONCE and share across methods that need it
        try:
            item_data = item.get_data()
        except Exception:
            item_data = None

        item_records += discover_via_itemgraph(item, item_lookup, origin_meta, found_pairs)
        item_records += discover_via_related_items(item, item_lookup, origin_meta, found_pairs)
        item_records += discover_via_data_scan(item, item_lookup, origin_meta, found_pairs, item_data=item_data)
        item_records += discover_via_operational_layers(item, item_lookup, origin_meta, found_pairs, item_data=item_data)
        item_records += discover_service_sublayers(item, origin_meta, found_pairs)
        item_records += discover_egdb_sources(item, origin_meta, found_pairs)
        item_records += discover_via_deep_url_scan(
            item, item_lookup, origin_meta, found_pairs, service_url_index, item_data=item_data
        )
        return item, origin_meta, item_records

    print(f"Scanning {len(items_to_process)} item(s) with {MAX_WORKERS} threads...\n")
    _counter_lock = threading.Lock()
    _progress = [0]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process_single_item, item): item for item in items_to_process}
        for future in as_completed(futures):
            with _counter_lock:
                _progress[0] += 1
                idx = _progress[0]

            try:
                item, origin_meta, item_records = future.result()
            except Exception as e:
                print(f"  [{idx}/{len(items_to_process)}] Error processing {futures[future].title}: {e}")
                continue

            items_scanned += 1
            num_relationships = len(item_records)

            if idx % 100 == 0 or idx == len(items_to_process):
                print(f"  [{idx}/{len(items_to_process)}] progress...")

            if num_relationships == 0:
                items_without_relationships += 1
                placeholder_record = {
                    "run_timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "origin_id": origin_meta["id"],
                    "origin_title": origin_meta["title"],
                    "origin_owner": origin_meta["owner"],
                    "origin_type": origin_meta["type"],
                    "origin_url": origin_meta["url"],
                    "origin_serviceUrl": origin_meta["serviceUrl"],
                    "origin_sharing": origin_meta["sharing"],
                    "origin_created": origin_meta["created"],
                    "origin_modified": origin_meta["modified"],
                    "dep_id": "NONE",
                    "dep_title": "NoRelationship",
                    "dep_owner": "NONE",
                    "dep_type": "NONE",
                    "dep_url": "NONE",
                    "dep_serviceUrl": None,
                    "dep_sharing": "NONE",
                    "dep_created": None,
                    "dep_modified": None,
                    "rel_type": "NoRelationship",
                    "rel_direction": "NONE",
                    "discovery_method": "none",
                    "has_relationship": False,
                    "dep_layer_id":   None,
                    "dep_layer_name": None,
                    "dep_layer_url":  None,
                    "datastore_name":  _datastore_cache.get(origin_meta["id"], {}).get("datastore_name"),
                    "service_layer_names": _service_layers_cache.get(origin_meta["id"]),
                    "source_egdb_datasets": _format_egdb_datasets(origin_meta["id"]),
                }
                all_records.append(placeholder_record)
            else:
                items_with_relationships += 1
                total_relationships += num_relationships
                for r in item_records:
                    r["has_relationship"] = True
                all_records.extend(item_records)

    end_time = datetime.datetime.now()
    duration = end_time - start_time

    stats = {
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "items_scanned": items_scanned,
        "total_relationships": total_relationships,
        "items_with_relationships": items_with_relationships,
        "items_without_relationships": items_without_relationships,
        "incremental": last_run_ts is not None,
        "environment": target,
    }

    print(f"\nTotal records (including no-relationship items): {len(all_records)}")
    return all_records, stats, changed_item_ids

# ------------------ LOG FILE ------------------

def write_log_file(stats, log_path):
    """Write a summary log file with run statistics."""
    hours, remainder = divmod(int(stats["duration"].total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    duration_str = f"{hours}h {minutes}m {seconds}s"
    mode_label = "Incremental" if stats.get("incremental") else "Full"

    env_label = stats.get("environment", "unknown").upper()

    lines = [
        "=" * 55,
        "  ArcGIS Item Dependency Scan — Run Log",
        "=" * 55,
        f"  Environment               : {env_label}",
        f"  Scan Mode                 : {mode_label}",
        f"  Start Time                : {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}",
        f"  End Time                  : {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}",
        f"  Total Duration            : {duration_str}",
        "-" * 55,
        f"  Items Scanned             : {stats['items_scanned']}",
        f"  Total Relationships Found : {stats['total_relationships']}",
        f"  Items With Relationships  : {stats['items_with_relationships']}",
        f"  Items Without Relationship: {stats['items_without_relationships']}",
        "=" * 55,
    ]

    log_text = "\n".join(lines) + "\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_text)
    print(f"\nLog written to: {log_path}")
    return log_text

# ------------------ DB INSERT ------------------

def delete_records_for_items(cursor, changed_ids):
    """Delete existing DB rows whose OriginItemID is in the changed set.
    This ensures stale relationships are removed before re-inserting."""
    if not changed_ids:
        return 0
    # Build parameterised IN clause
    placeholders = ",".join("?" for _ in changed_ids)
    sql = f"DELETE FROM {DB_TABLE} WHERE OriginItemID IN ({placeholders})"
    cursor.execute(sql, list(changed_ids))
    deleted = cursor.rowcount
    return deleted


def export_to_database(records, changed_item_ids=None):
    print(f"\nConnecting to SQL Server: {DB_SERVER} / {DB_DATABASE}")
    conn   = get_db_connection()
    create_table_if_not_exists(conn)
    cursor = conn.cursor()

    # For full runs, purge the entire table so stale records don't persist
    if not changed_item_ids:
        print(f"Full scan — deleting all existing records from {DB_TABLE}...")
        cursor.execute(f"DELETE FROM {DB_TABLE}")
        deleted = cursor.rowcount
        conn.commit()
        print(f"  Deleted {deleted} old record(s).")
    else:
        # For incremental runs, remove old records for changed items first
        deleted = delete_records_for_items(cursor, changed_item_ids)
        print(f"Incremental update — deleted {deleted} old record(s) for {len(changed_item_ids)} changed item(s).")
        conn.commit()

    inserted = 0
    skipped  = 0
    BATCH_SIZE = 500

    for i, r in enumerate(records, 1):
        try:
            insert_record(cursor, r)
            inserted += 1
        except pyodbc.IntegrityError:
            skipped += 1  # duplicate hit the UNIQUE constraint
        except Exception as e:
            print(f"  Insert error for {r['origin_id']} → {r['dep_id']}: {e}")
            skipped += 1

        # Commit in batches to reduce transaction overhead
        if i % BATCH_SIZE == 0:
            conn.commit()

    conn.commit()
    conn.close()
    print(f"DB insert complete — {inserted} inserted, {skipped} skipped (duplicates).")

# ------------------ MAIN ------------------

def _setup_environment(env):
    """Authenticate and set module globals for the given environment."""
    global target, gis, PORTAL_URL, DB_TABLE
    target = env

    if env == "agol":
        userName = "YOUR_AGOL_USERNAME"
        password = keyring.get_password("arcgis_AGOL", userName)
        gis = GIS("https://your-org.maps.arcgis.com/home", userName, password)
        PORTAL_URL = "https://your-org.maps.arcgis.com"
        DB_TABLE = "dbo.ItemDependenciesAGOL"
    else:
        userName = "YOUR_PORTAL_ADMIN_USERNAME"
        password = keyring.get_password("arcgis_portal", userName)
        gis = GIS("https://your-portal-url/portal", userName, password)
        PORTAL_URL = "https://your-portal-url/portal"
        DB_TABLE = "dbo.ItemDependenciesPortal"

    print(f"Target: {target.upper()}")
    print(f"Authenticated as: {gis.users.me.username}")

def emailconfig(SUBJECT,TO,logFilePath,inputMsg):
    SERVER = "<your-email-server>"
    FROM = "<your-emai-address>"
    MSG = "Log File: {}.log\n\n{}".format(logFilePath,inputMsg)
    # Prepare actual message
    MESSAGE = """\
From: %s
To: %s
Subject: %s

%s
""" % (FROM, TO, SUBJECT, MSG)
    server = smtplib.SMTP(SERVER)
    server.sendmail(FROM, TO, MESSAGE)
    server.quit()


def _run_scan():
    """Determine scan mode, discover dependencies, export, and email."""
    try:
        # Determine scan mode
        last_run_check = get_last_run_timestamp(LOG_FILE, target)
        if last_run_check:
            print(f"Last {target.upper()} run ended at: {last_run_check.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            scan_mode = "incremental"
        else:
            print(f"No previous {target.upper()} run found — running full scan.")
            scan_mode = "full"

        if scan_mode == "full":
            last_run_ts = None
            print("Full scan mode selected.\n")
        else:
            last_run_ts = last_run_check
            print(f"Incremental mode — scanning items changed since {last_run_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC\n")

        records, stats, changed_item_ids = discover_all_dependencies(last_run_ts)

        # Step 1: Write the log file
        current_run_log = write_log_file(stats, LOG_FILE)

        # Step 2: Insert into DB
        export_to_database(records, changed_item_ids if stats["incremental"] else None)

        # Step 3: Email the log summary
        mode_label = "Incremental" if stats["incremental"] else "Full"
        duration_message = durationEnd(scriptStartTime)
        email_body = (
            f"Mode: {mode_label}\n"
            f"Environment: {target.upper()}\n"
            f"Log File: {os.path.abspath(LOG_FILE)}\n"
            f"Duration: {duration_message}\n\n"
            f"{current_run_log}"
        )
        email_body = email_body.encode("ascii", errors="replace").decode("ascii")
        emailconfig(
            f"Success: {target.upper()} Data Dependencies Processed",
            toEmails, "", email_body,
        )

    except Exception as scan_exception:
        error_msg = f"Error during {target.upper()} scan: {scan_exception}"
        print(error_msg)
        logWrite(LOG_FILE, error_msg)
        emailconfig(
            f"FAILED: {target.upper()} Data Dependencies Scan",
            toEmails, "", error_msg,
        )
        raise


def Portal():
    try:
        text = "Portal Item Dependencies Scan"
        logWrite(LOG_FILE, f"START: {text}")

        _setup_environment("portal")
        _run_scan()

        duration_message = durationEnd(scriptStartTime)
        logWrite(LOG_FILE, f"Completed successfully in {duration_message}")
        logWrite(LOG_FILE, f"END: {text}\n")

    except Exception as main_exception:
        error_msg = f"Error in Portal scan: {main_exception}"
        print(error_msg)
        logWrite(LOG_FILE, error_msg)
        emailconfig(
            "FAILED: Portal Data Dependencies Scan",
            toEmails, "", error_msg,
        )


def Agol():
    try:
        text = "AGOL Item Dependencies Scan"
        logWrite(LOG_FILE, f"START: {text}")

        _setup_environment("agol")
        _run_scan()

        duration_message = durationEnd(scriptStartTime)
        logWrite(LOG_FILE, f"Completed successfully in {duration_message}")
        logWrite(LOG_FILE, f"END: {text}\n")

    except Exception as main_exception:
        error_msg = f"Error in AGOL scan: {main_exception}"
        print(error_msg)
        logWrite(LOG_FILE, error_msg)
        emailconfig(
            "FAILED: AGOL Data Dependencies Scan",
            toEmails, "", error_msg,
        )


def main():
    try:
        if len(sys.argv) < 2:
            logWrite(LOG_FILE, "Please provide a command-line argument (Portal or Agol)")
            print("Usage: python DependenciesMainAll.py <Portal|Agol>")
            return

        option = sys.argv[1]

        if option == "Portal":
            Portal()
        elif option == "Agol":
            Agol()
        else:
            print(f"Invalid option: {option}. Please provide Portal or Agol.")
            logWrite(LOG_FILE, f"Invalid option: {option}")

    except Exception as main_exception:
        error_msg = f"Error in main function: {main_exception}"
        print(error_msg)
        logWrite(LOG_FILE, error_msg)


if __name__ == "__main__":
    main()
