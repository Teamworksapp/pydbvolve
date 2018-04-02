# GLOBAL

VALID_COLUMNS = [
    'id', 'migration_id', 'requested_ts', 'applied_ts', 'migration_file', 'migration_action', 'migration_type', 'migration_description', 
    'migration_user', 'db_user', 'is_current', 'is_baseline', 'is_active'
]
VALID_ACTIONS = {'upgrade', 'downgrade', 'baseline', 'info', 'verify', 'log', 'create', 'destroy', 'update-log', 'rebuild-log', 'export-log'}
LATEST_VERSION = '\x00LATEST\x00'
CURRENT_VERSION = '\x00CURRENT\x00'
BASELINE_VERSION = '\x00BASELINE\x00'
NEW_VERSION = '\x00NEW\x00'
_BASE_VALUE_LENGTHS = [10, 28, 28, 25, 8, 7, 15, 15, 15, 5, 5]
COLUMN_LENGTHS = [max((_BASE_VALUE_LENGTHS[i], len(VALID_COLUMNS[i]))) for i in range(len(VALID_COLUMNS))]


