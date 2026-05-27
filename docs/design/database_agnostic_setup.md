# Design: Database-Agnostic Setup Hooks for iRODS

## 1. Problem Statement
The current iRODS setup and verification process is heavily coupled to relational databases. 
- **Python Setup:** `setup_irods.py` and `database_interface.py` assume the presence of `pyodbc` and execute hardcoded SQL scripts to create tables and insert default values.
- **Server Startup:** `main.cpp` performs a manual SQL query via `nanodbc` to check the `schema_version` before allowing the server to boot.
- **Relational Hardcoding:** Core logic in `atomic_apply_database_operations.cpp` and `rs_genquery2.cpp` assumes a relational table structure.

This coupling prevents non-relational plugins (e.g., the L3KVG Graph Plugin) from being installed and verified using standard iRODS procedures.

## 2. Core Principles
- **Plugin Sovereignty:** The database plugin, not the iRODS core, should own the knowledge of its internal schema and how to initialize it.
- **Agnostic Verification:** The iRODS core should verify catalog state through standardized plugin operations rather than direct SQL queries.
- **Unified Management:** Setup scripts should interact with a unified management interface that abstracts the underlying storage technology.

## 3. Proposed "Setup Hooks" (Plugin Operations)

I propose adding the following operations to the iRODS Database Plugin interface:

### 3.1. `DATABASE_OP_GET_CATALOG_VERSION`
- **Purpose:** Returns the current semantic version of the catalog schema.
- **Input:** None.
- **Output:** Integer version number.
- **SQL Implementation:** `SELECT option_value FROM R_GRID_CONFIGURATION WHERE ...`
- **L3KVG Implementation:** Returns version from a metadata node.

### 3.2. `DATABASE_OP_INITIALIZE_CATALOG`
- **Purpose:** Performs the initial bootstrap of a fresh catalog.
- **Input:** JSON object containing `zone_name`, `admin_user`, `admin_password_hash`, etc.
- **Output:** irods::error status.
- **SQL Implementation:** Executes `icatSysTables.sql` and `icatSysInserts.sql`.
- **L3KVG Implementation:** Creates initial graph nodes and edges.

### 3.3. `DATABASE_OP_VERIFY_INTEGRITY`
- **Purpose:** Runs plugin-specific sanity checks (e.g., checking for mandatory indices or tables).
- **Input:** None.
- **Output:** irods::error status.

## 4. Architectural Changes

### 4.1. Core iRODS Server (`main.cpp`)
Refactor `check_catalog_schema_version()` to:
1. Load the configured database plugin via `database_manager`.
2. Call `DATABASE_OP_GET_CATALOG_VERSION`.
3. Compare the result with the expected version in `version.json`.

### 4.2. Python Setup Layer (`database_interface.py`)
1. Detect if the plugin is "Modern Agnostic" (supports initialization operation).
2. If so, instead of running SQL via `pyodbc`, use a new iRODS CLI management command (e.g., `irods-db-admin init`) that loads the plugin and triggers the bootstrap.

### 4.3. Atomic Database Operations
Refactor `atomic_apply_database_operations.cpp` to use a generic **Entity Mapper**. Instead of hardcoding `r_data_main`, the code will request a write to an `EntityType::DataObject`. The plugin then maps this to a table name or a node label.

## 5. Success Criteria
- `setup_irods.py` successfully installs an iRODS instance using the L3KVG plugin without `pyodbc` being present on the system.
- `irodsServer` boots successfully by verifying the catalog version through the plugin interface.
- No hardcoded SQL strings remain in the core `server/` or `lib/` directories.
