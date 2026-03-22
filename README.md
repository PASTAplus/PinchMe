# PinchMe
A PASTA data package and resource integrity checker.

PinchMe is a specialized integrity scanner designed for the PASTA resource registry. It ensures the physical integrity of data packages and their associated resources (metadata, reports, and data entities) by verifying their checksums and sizes against the values recorded in the registry.

## Core Purpose
The application automates the process of identifying new or updated data packages in a remote PASTA database, syncing their metadata to a local tracking pool, and performing rigorous file-system-level validation to detect bit rot, accidental deletions, or unauthorized modifications.

## Key Components

### 1. Registry Interface (`pasta_db.py`, `pasta_resource_registry.py`)
- Connects to the remote PASTA PostgreSQL database.
- Executes optimized SQL queries to fetch package identifiers, creation dates, and resource metadata (MD5, SHA1, byte size, and physical location).

### 2. Local Validation Pool (`resource_db.py`)
- Managed via a local SQLite database using SQLAlchemy 2.0.
- Tracks the state of all packages and resources, including validation status, check counts, and the last check date/status.
- Models:
    - **Packages:** Tracks unique EDI identifiers and their global creation dates.
    - **Resources:** Tracks individual files (metadata, data, reports), their expected checksums, and validation history.

### 3. Synchronization Engine (`package_pool.py`)
- Bridges the remote registry and the local pool.
- Discovers new packages created since the last local sync.
- Populates the local pool with resource-level details for every discovered package.

### 4. Validation Logic (`validation.py`)
- Performs the actual integrity checks:
    - **Checksum Verification:** Re-computes MD5 and SHA1 hashes of files on disk and compares them to the registry's recorded values.
    - **Size Verification:** Confirms the file size on disk matches the expected byte count.
- **Error Codes:**
    - `0b0001`: MD5 Mismatch
    - `0b0010`: SHA1 Mismatch
    - `0b0100`: Size Mismatch
    - `0b1000`: File Not Found

### 5. Notification & Locking (`mimemail.py`, `lock.py`)
- **Email Alerts:** Automatically sends notifications when integrity errors are detected.
- **Process Locking:** Uses file-based locking to prevent multiple instances of PinchMe from running concurrently.

## Operational Workflow

1.  **Discovery:** The application queries the remote registry for new packages.
2.  **Sync:** Discovered package metadata is inserted into the local SQLite pool.
3.  **Validation:**
    - The application iterates through unvalidated packages/resources.
    - It resolves the physical file path for each resource.
    - It performs I/O-intensive checksum and size calculations.
4.  **Reporting:** Results are logged to the local database, printed to stdout (if verbose), and emailed if failures occur.

## CLI Usage

```shell
Usage: pinchme [OPTIONS]

  PinchMe

  A PASTA data package and resource integrity checker. Running "pinchme" without any options will
  validate all new and existing data packages and resources, and then exit.

Options:
  -a, --algorithm TEXT   Package pool selection algorithm: either
                         'create_ascending', 'create_descending',
                         'id_ascending',  'id_descending', or 'random'
                         (default).
  -b, --bootstrap        Create a new resource database and run validation
                         against all packages.
  -d, --delay INTEGER    Delay (seconds) between package integrity checks.
  -e, --email            Email on integrity error.
  -f, --failed           Rerun integrity checks against all failed resources,
                         then exit.
  -i, --identifier TEXT  Add specified package(s) to validation pool, then
                         exit.
  -l, --limit INTEGER    Limit the number of new packages added to the
                         validation pool.
  -p, --pool             Add new packages to validation pool, then exit.
  -r, --reset            Reset validated flag on all packages/resources, then
                         exit.
  -s, --show             Show all failed resources. Error codes: 0b0001=MD5 ||
                         0b0010=SHA1 || 0b0100=SIZE || 0b1000=NOTFOUND.
  -S, --stop             Stop ongoing validation process and exit.
  -u, --update TEXT      Update package(s) with new metadata from resource
                         registry.
  -v, --validate TEXT    Validate specified package(s), then exit.
  -V, --verbose          Print activity to stdout.
  -h, --help             Show this message and exit.
```
---
*Co-authored-by: Gemini CLI <gemini-cli@google.com>*

