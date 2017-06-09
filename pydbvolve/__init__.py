# ====================================
#  All recurds returned by database calls should be of dict type or behave exactly like dict!!
# ====================================

__VERSION__ = (1, 0, 1)
__VERSION_STRING__ = '.'.join(str(v) for v in __VERSION__)


# Uncomment for debugging
# try:
#     from IPython import embed
# except:
#     pass

import sys
import os
import re
from datetime import datetime as dt
import traceback
import importlib
import importlib.machinery as ilmac
import importlib.util as ilutil
import logging

# columns in the migrations table
VALID_COLUMNS = [
    'version', 'applied_ts', 'migration_file', 'migration_action', 'migration_type',
    'migration_user', 'db_user', 'is_current', 'is_baseline'
]
VALID_ACTIONS = {'upgrade', 'downgrade', 'baseline', 'info', 'verify', 'log'}
LATEST_VERSION = '\x00LATEST\x00'

LOG_FORMAT = '%(asctime)s %(levelname)s %(migration_user)s: %(message)s'
LOG_ECHO_FORMAT = '%(asctime)s: %(message)s'

_BASE_VALUE_LENGTHS = [10, 28, 25, 8, 7, 15, 15, 5, 5]
COLUMN_LENGTHS = [max((_BASE_VALUE_LENGTHS[i], len(VALID_COLUMNS[i]))) for i in range(len(VALID_COLUMNS))]


class MigrationTableOutOfSync(Exception):
    pass
# End MigrationTableOutOfSync


class MigrationTableConstraintError(Exception):
    pass
# End MigrationTableOutOfSync


class MigrationError(Exception):
    pass
# End MigrationTableOutOfSync


def pre_execution(config):
    """
    Called after initialization but before processing. Accepts the config dict as the sole argument.
    Overide this function in your config file for custom action.
    """
    
    return None
# End pre_execution


def post_execution(config):
    """
    Called after processing but before exit. Accepts the config dict as the sole argument.
    Overide this function in your config file for custom action.
    """
    
    return None
# End post_execution


def pre_script(config, migration):
    """
    Called after script is resolved, but before execution. Accepts config dict and migration dict as args.
    Overide this function in your config file for custom action.
    """
    
    return None
# End pre_script


def post_script(config, migration):
    """
    Called after script is executed. Accepts config dict and migration dict as args.
    Overide this function in your config file for custom action.
    """
    
    return None
# End post_script


def pre_statement(config, migration, statement):
    """
    Called on SQL migrations only. Called before individual statement execution.
    Accepts config dict, migration dict, and statement string as arguments.
    Overide this function in your config file for custom action.
    """
    
    return None
# End pre_statement


def post_statement(config, migration, statement):
    """
    Called on SQL migrations only. Called after individual statement execution.
    Accepts config dict, migration dict, and statement string as arguments.
    Overide this function in your config file for custom action.
    """
    
    return None
# End post_statement


def get_migration_user(config):
    """
    Returns the username of the program executor.
    """
    import getpass
    return getpass.getuser()
# End get_migration_user


def get_database_user(config, credentials):
    """
    Returns the database user from the credentials.
    """
    return credentials.get('user', 'unknown')
# End get_database_user


def get_base_dir():
    """
    Returns the base directory path. Default is './pydbvolve'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join('.', 'pydbvolve')
# End get_base_dir


def get_migration_base_dir():
    """
    Returns the base directory for the migrations. Default is get_base_dir() + '/migrations'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(get_base_dir(), "migrations")
# End get_migration_dir


def get_migration_upgrade_dir():
    """
    Returns the base directory for the migrations. Default is get_migration_base_dir() + '/upgrades'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(get_migration_base_dir(), "upgrades")
# End get_migration_upgrade_dir


def get_migration_downgrade_dir():
    """
    Returns the base directory for the migrations. Default is get_migration_base_dir() + '/downgrades'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(get_migration_base_dir(), "downgrades")
# End get_migration_downgrade_dir


def get_log_dir():
    """
    Returns the base directory for the migrations. Default is get_base_dir() + '/logs'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(get_base_dir(), "logs")
# End get_log_dir


def get_migration_table_name():
    """
    Returns the name of the table that will store the migration run records. Default is '__migrations__'.
    Overide this function in your config file to set a custom directory.
    If your database does not have a schema type, just have this function return empty string.
    """
    
    return '__migrations__'
# End get_migration_table_name


def get_migration_table_schema():
    """
    Returns the name of the schema in which to create/use the migrations table. Default is 'public'.
    Overide this function in your config file to set a custom directory.
    If your database does not have a schema type, just have this function return empty string.
    """
    
    return 'public'
# End get_migration_table_schema


def get_positional_variable_marker():
    """
    Returns the name of the schema in which to create/use the migrations table. Default is 'public'.
    Overide this function in your config file to set a custom directory.
    If your database does not have a schema type, just have this function return empty string.
    """
    
    return '%s'
# End get_positional_variable_marker


def set_log_file_name(config):
    """
    Returns a formatted log file name using values from the config (log_dir, version, migration_action) and current datetime
    """
    
    version = config['version']
    if version == LATEST_VERSION:
        version = 'latest'
    
    config['log_file_name'] = os.path.join(config.get('log_dir', '.'), '.'.join((version.replace(' ', '_'), config['migration_action'].replace(' ', '_'), dt.now().strftime('%Y-%m-%d_%H:%M:%S'), 'log')))
    
    return config
# End set_log_file_name


def set_log_level(config):
    """
    Sets the default log level in the config dict. (logging.INFO)
    """
    
    config['logger_level'] = logging.INFO
    
    return config
# End set_log_level


def set_logger_name(config):
    """
    Sets the logger name in the config dict. ('pydbvovle')
    """
    
    config['logger_name'] = 'pydbvolve'
    
    return config
# End set_logger_name


def setup_error_log_handler(config):
    """
    Setup and add a logging handler specifically for logging.ERROR messages.
    """
    
    log = config.get('logger')
    if log:
        formatter = logging.Formatter(LOG_FORMAT)
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(formatter)
        log_handler.setLevel(logging.WARNING)
        log.addHandler(log_handler)
    
    return config
# End setup_error_log_handler


def setup_file_logger(config):
    """
    Setup a file-based primary logger and set it in the config dict.
    Also (optionally) set a verbose handler (controlled with by config['verbose']).
    Also set a separate error handler.
    """
    
    # Pirmary logger
    formatter = logging.Formatter(LOG_FORMAT)
    log_handler = logging.FileHandler(config.get('log_file_name', 'pydbvolve.log'))
    log_handler.setFormatter(formatter)
    log_handler.setLevel(config.get('log_level', logging.INFO))
    logger = logging.getLogger(config.get('logger_name', 'pydbvolve'))
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handler)
    
    # Verbosity logger
    if config.get('verbose'):
        formatter = logging.Formatter(LOG_ECHO_FORMAT)
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(formatter)
        log_handler.setLevel(logging.INFO)
        logger.addHandler(log_handler)
    
    # Errors logger
    config['logger'] = logger
    setup_error_log_handler(config)
    
    return config
# End setup_file_logger


def setup_stream_logger(config):
    """
    Setup a stream-based logger and set it in the config dict.
    """
    
    logging.basicConfig(format=LOG_FORMAT, level=config.get('log_level', logging.INFO))
    formatter = logging.Formatter(LOG_FORMAT)
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(formatter)
    log_handler.setLevel(config.get('log_level', logging.INFO))
    logger = logging.getLogger(config.get('logger_name', 'pydbvolve'))
    logger.addHandler(log_handler)
    
    config['logger'] = logger
    
    return config
# End setup_root_logger


def setup_log(config):
    """
    Sets the config for logging and creates the logger instance for pydbvolve
    """
    
    set_log_file_name(config)
    set_log_level(config)
    set_logger_name(config)
    
    if config.get('log_file_name'):
        setup_file_logger(config)
    else:
        setup_stream_logger(config)
    
    return config
# End open_log_file


def close_log(config):
    """
    Flush and close the handlers
    """
    
    log = config.get('log')
    if log:
        for h in log.handlers:
            h.flush()
            h.close()
            log.removeHandler(h)
# End close_log


def write_log(config, message, level=None):
    """
    Log the message using the config
    """
    
    log = config.get('logger')
    if log:
        log.log((level or config.get('log_level', logging.INFO)), message, extra=config)
# End write_log


def get_db_credentials(config):
    """
    Override this function to return the credentials as a dict that are necessary to connecto to your database.
    If you have specific security for your credentials, this is where you should resolve it.
    You should always have a key named 'user' in the dict that will be the username (str) of the database user.
    Always return a dict from this function.
    Arguments are the config dict.
    """
    
    raise NotImplementedError("You must implement a function named 'get_db_credentials' in your config file.")
# End get_db_credentials


def get_db_connection(config, credentials):
    """
    Override this function to use the credentials to establish a connection to your database.
    The return from this function should always be a connection instance.
    All records returned from cursors created from this connection must be of type dict or subclassed from dict.
    Args are the config dict and the credentials dict.
    """
    
    raise NotImplementedError("You must implement a function named 'get_db_connection' in your config file.")
# End get_db_connection    


def get_filename_regex():
    """
    Returns a regex instance (re.compile() result) that will be used to parse the filenames 
    to get version, description, and type information (in that exact order).
    Overide this function in your config file to set a custom regex.
    """
    
    return re.compile('^([^_]+)_([^.]+).(sql|py)$')
# End get_file_regex


def get_sort_version(config, version):
    """
    Returns a form of the version obtained from the execution of get_migration_filename_info() call that can be properly sorted.
    """
    
    noAlpha = re.compile('[^0-9.]+')
    return tuple(int(x) for x in noAlpha.sub('', version).split('.'))
# End get_sort_version


def get_migration_filename_info(config, fileName):
    """
    Returns a dict with information about the migration filename obtained by using the regex returned from get_filename_regex() call:
    {
        version: # version string
        description: # description
        filetype: # file type. Currently only '.sql' and '.py' filetypes are supported.
        filename: input filename,
        sort_version: form of the version string that is sortable (See get_sort_version())
    }
    If there is an error with the regex, None will be returned.
    Regex output length should always return a list or tuple of length 3.
    """
    
    keys = ['version', 'description', 'filetype']
    # findall returns a list. The regex result (if found) will be a tuple
    values = config['filename_regex'].findall(os.path.basename(fileName))
    if values:
        values = values[0]
    if len(values) == len(keys):
        info = dict(zip(keys, values))
        info['filename'] = fileName
        info['sort_version'] = get_sort_version(config, info['version'])
        return info
    else:
        return None
# End get_migration_filename_info


def sort_migrations(config, migrations, reverse=False):
    """
    Returns a sorted version of the migrations list
    """
    
    migrations.sort(key=lambda x: x['sort_version'], reverse=reverse)
    return migrations
# End sort_migrations


def get_config():
    """
    Build a config dict from the loaded config Python file.
    """
    
    schema = get_migration_table_schema()
    if schema:
        schema = '"{}".'.format(schema)
    
    config = {
        'base_dir': get_base_dir(),
        'migration_dir': get_migration_base_dir(),
        'migration_upgrade_dir': get_migration_upgrade_dir(),
        'migration_downgrade_dir': get_migration_downgrade_dir(),
        'filename_regex': get_filename_regex(),
        'log_dir': get_log_dir(),
        'migration_table_schema': schema,
        'migration_table_name': get_migration_table_name(),
        'positional_variable_marker': get_positional_variable_marker()
    }
    
    return config
# End get_config


def confirm_dirs(config):
    """
    Verify that config directories exist and created them if they do not.
    """
    
    for k, v in config.items():
        if k.endswith('_dir'):
            os.makedirs(v, mode=0o755, exist_ok=True)
# End confirm_dirs


def load_config(configFileName):
    """
    Load and execute the config Python file.
    """
    
    with open(configFileName, 'r') as configFile:
        co = compile(configFile.read(), configFileName, 'exec')
    
    if co:
        exec(co, globals(), globals())
# End load_config


def check_migration_table(config):
    """
    Returns bool
    Verifies existence, structure, and unique record flags of migrations table and its data.
    """
    
    conn = config['conn']
    validCols = set(VALID_COLUMNS)
    sql = """
select * 
  from {}"{}"
 where 1 = 0;
""".format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    with conn.cursor() as cur:
        try:
            write_log(config, "Checking existence of migrations table")
            cur.execute(sql)
        except Exception as e:
            write_log(config, 'The {}"{}" table does not exist'.format(config.get('schema', ''), config['migration_table_name']))
            rc = False
        else:
            write_log(config, "Checking migrations table structure")
            gotCols = {c[0] for c in cur.description}
            if gotCols != validCols:
                raise MigrationTableOutOfSync('The {}"{}" table structure is out-of-date: cols=({}); valid=({})'.format(config.get('migration_table_schema', ''), config['migration_table_name'], sorted(gotCols), sorted(validCols)))
            
            sql = """
select count(*) as "count"
  from {}"{}"
 where is_current = 1;
""".format(config.get('migration_table_schema', ''), config['migration_table_name'])
            write_log(config, "Checking migrations table data")
            cur.execute(sql)
            res = cur.fetchone()
            if res['count'] > 1:
                raise MigrationTableConstraintError('The {}"{}" table data has violated a constraint. There are {} current versions when there should only be 1'.format(config.get('schema', ''), config['migration_table_name'], res['count']))
            
            sql = sql.replace('current', 'baseline')
            cur.execute(sql)
            res = cur.fetchone()
            if res['count'] > 1:
                raise MigrationTableConstraintError('The {}"{}" table data has violated a constraint. There are {} baseline versions when there should only be 1'.format(config.get('schema', ''), config['migration_table_name'], res['count']))
            
            rc = True
        finally:
            conn.rollback()
        # End try/except/else/finally block to check the state of the migration table
    
    return rc
# End check_migration_table


def get_baseline(config):
    """
    Returns a dict representing the baseline migration record or empty dict if none exist.
    """
    
    conn = config['conn']
    try:
        write_log(config, "Getting baseline version")
        with conn.cursor() as cur:
            cur.execute("""select * from {}"{}" where is_baseline = 1""".format(config.get('migration_table_schema', ''), config['migration_table_name']))
            res = cur.fetchone()
            if res is None:
                res = {}
    except Exception as e:
        write_log(config, 'EXCEPTION:: getting baseline version {}'.format(e), level=logging.ERROR)
        return {}
    
    return res
# End get_baseline


def clear_baseline(config):
    """
    Unset the baseline flag on the baseline record
    """
    
    conn = config['conn']
    sql = """
update {}"{}"
   set is_baseline = 0
 where is_baseline = 1;
""".format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    write_log(config, "Clearing baseline version")
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
        except Exception as e:
            write_log(config, "EXCEPTION:: reset of baseline flag failed! {}\n Stmt:\n{}".format(e, sql), level=logging.ERROR)
            return False
        else:
            return True
# End clear_baseline


def get_current(config):
    """
    Returns a dict representing the baseline migration record or empty dict if none exist.
    """
    
    conn = config['conn']
    try:
        write_log(config, "Getting current version")
        with conn.cursor() as cur:
            cur.execute("""select * from {}"{}" where is_current = 1""".format(config.get('migration_table_schema', ''), config['migration_table_name']))
            res = cur.fetchone()
            if res is None:
                res = {}
    except Exception as e:
        write_log(config, 'EXCEPTION:: getting current version {}'.format(e), level=logging.ERROR)
        return {}
    
    return res
# End get_current


def clear_current(config):
    """
    Unset the current flag on the baseline record
    """
    
    conn = config['conn']
    sql = """
update {}"{}"
   set is_current = 0
 where is_current = 1;
""".format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    write_log(config, "Clearing current version")
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
        except Exception as e:
            write_log(config, "EXCEPTION:: reset of current flag failed! {}\n Stmt:\n{}".format(e, sql), level=logging.ERROR)
            return False
        else:
            return True
# End clear_baseline


def add_migration_record(config, migration, current=0, baseline=0):
    """
    Adds a migration record to the migrations table. 
    If it is a baseline record, the existing baseline will be unset. 
    If it is a current record, the existing current will be unset.
    Returns bool
    """
    
    if current == 1:
        if not clear_current(config):
            return False
    if baseline == 1:
        if not clear_baseline(config):
            return False
    if current == 0 and baseline == 0:
        write_log(config, "ERROR:: The flags 'current' and 'baseline' cannot both be zero (0)", level=logging.ERROR)
        return False
        
    conn = config['conn']
    sql = """
insert 
  into {}"{}"
       (
         {}
       )
values (
         {}
       )
""".format(config.get('migration_table_schema', ''), config['migration_table_name'], ', '.join(VALID_COLUMNS), ', '.join([config['positional_variable_marker']] * len(VALID_COLUMNS)))
    valuesd = dict.fromkeys(VALID_COLUMNS)
    for k in VALID_COLUMNS:
        valuesd[k] = config.get(k)
    
    valuesd['is_current'] = current
    valuesd['is_baseline'] = baseline
    valuesd['applied_ts'] = dt.now()
    valuesd['migration_file'] = os.path.basename(migration.get('filename', ''))[:256]
    valuesd['migration_type'] = migration.get('filetype', '')
    valuesd['version'] = migration.get('version', config['version'])
    
    values = tuple(valuesd[c] for c in VALID_COLUMNS)
    
    write_log(config, "Adding migration record for version {}; baseline = {}; current = {}".format(valuesd['version'], valuesd['is_baseline'], valuesd['is_current']))
    with conn.cursor() as cur:
        try:
            cur.execute(sql, values)
        except Exception as e:
            write_log(config, "EXCEPTION:: {}\nRunning statement\n{} {}".format(e, sql, values))
            raise e
    return True
# End add_migration_record


def set_baseline(config):
    """
    Set a baseline record in the database. 
    The baseline is the absolute marker for the past. 
    You cannot downgrade past the baseline. 
    You cannot upgrade to a target before the start or before the baseline.
    """
    
    conn = config['conn']
    try:
        baseline = get_baseline(config)
        if bool(baseline) and (baseline['version'] == config['version']):
            write_log(config, "Baseline version '{}' has already been set".format(config['version']))
            conn.rollback()
            return 0    # Exit with no error
        else:
            write_log(config, "Setting baseline at version {}".format(config['version']))
            current = get_current(config) # We don't want to overwrite an existing current record if we're retroactively baselining
            addOK = add_migration_record(config, {}, baseline=1, current=(0 if bool(current) else 1))
    except Exception as e:
        write_log(config, "EXCEPTION:: set_baseline failed! {}".format(e), level=logging.ERROR)
        conn.rollback()
        return 10
    
    if addOK:
        conn.commit()
        return 0
    else:
        conn.rollback()
        return 11
# End set_baseline


def create_migration_table(config):
    """
    Creates the migration table in the specified schema.
    """
    
    conn = config['conn']
    schema = config.get('migration_table_schema', '')
    tableName = config['migration_table_name']
    sql = """
create table {}"{}"
(
    version          varchar(256) not null,  -- version string
    applied_ts       timestamp not null,     -- time version was successfully applied
    migration_file   varchar(256) not null,  -- file name of migration run
    migration_action varchar(256) not null,  -- 'upgrade', 'downgrade', etc
    migration_type   varchar(256) not null,  -- 'python', 'sql', ...
    migration_user   varchar(256) not null,  -- name of user running migration program
    db_user          varchar(256) not null,  -- name of database user applying sql statements
    is_current       integer not null check (is_current in (0, 1)), -- flag for current version
    is_baseline      integer not null check (is_baseline in (0, 1)) -- flag for baseline version
);
""".format(schema, tableName)
    
    indexes = [
        """create unique index ux01__migrations__ on {}"{}" (version, applied_ts);""".format(schema, tableName),
        """create index ix01__migrations__ on {}"{}" (is_current);""".format(schema, tableName),
        """create index ix02__migrations__ on {}"{}" (is_baseline);""".format(schema, tableName)
    ]
    
    write_log(config, "Creating migration table")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            for sql in indexes:
                cur.execute(sql)
    except Exception as e:
        conn.rollback()
        write_log(config, "EXCEPTION creating migrations table: {}".format(e))
        if config.get('verbose', False):
            traceback.print_exc(file=sys.stderr)
        rc = False
    else:
        conn.commit()
        rc = True
    
    return rc
# End create_migration_table


def import_arbitrary(fileName, modName):
    """
    Returns a module reference.
    This function acts like a Python import statement, but it will 
    import an arbitrary Python file located at any path.
    """
    
    importlib.invalidate_caches()
    
    loader = ilmac.SourceFileLoader(modName.replace('.', '_'), fileName)
    spec = ilutil.spec_from_loader(loader.name, loader)
    mod = ilutil.module_from_spec(spec)
    loader.exec_module(mod)
    
    return mod
# End import_arbitrary


def run_python_migration(config, migration):
    """
    Returns bool.
    Loads and runs a Python migration as a module. 
    The Python migration module should contain a function named 'run_config' 
    that accepts two positional arguments for the config dict and the migration dict.
    """
    
    # Expose the write_log func to the migration
    config['write_log'] = write_log
    
    # Expose the base migration exception to the migration
    config['migration_exception'] = MigrationError
    
    write_log(config, 'Loading python migration "{}"'.format(migration['filename']))
    migration_module_name = 'pv_mg_' + migration['version']
    pymigration = import_arbitrary(migration['filename'], migration_module_name)
    
    if hasattr(pymigration, 'run_migration'):
        write_log(config, 'Running python migration (run_migration() call)'.format(migration['filename']))
        rc = pymigration.run_migration(config, migration)
    else:
        write_log(config, "Migration was run at import-time")
        rc = True
    
    # unload migration
    # This might be technically unnecessary, but I want to be explicit about this
    # in case behavior changes in future
    if migration_module_name in sys.modules:
        del sys.modules[migration_module_name]
    del pymigration
    
    return rc
# End run_python_migration


def get_sql_statement_sep():
    """
    Returns SQL statement separator regex.
    Default is '--run' on its own line.
    """
    
    return re.compile('^\s*--\s*run\s*$', flags=re.MULTILINE|re.IGNORECASE)
# End get_sql_statement_sep


def get_statements(sqlFile):
    """
    Returns str
    Reads 1-MiB chunks of a sql file and parses statements from each chunk. 
    Each statement found is yielded so this function is a generator.
    SQL migration statements are delimited by get_sql_statement_sep().
    """
    
    readLimit = 1000000
    stmtSep = get_sql_statement_sep()
    
    buff = sqlFile.read(readLimit)
    while buff:
        start = 0
        end = 0
        for m in stmtSep.finditer(buff):
            end = m.start()
            stmt = buff[start:end]
            start = m.end()
            yield stmt
        
        buff = buff[start:]
        newBuff = sqlFile.read(readLimit)
        if len(newBuff) == 0:
            break
        buff += newBuff
        del newBuff
    # End process loop
# End get_statements


def run_sql_migration(config, migration):
    """
    Returns bool
    Runs all statements in a SQL migration file one-at-a-time. Uses get_statements as a generator in a loop.
    """
    
    conn = config['conn']
    
    write_log(config, "SQL migration from file '{}'".format(migration['filename']))
    
    with open(migration['filename'], 'r') as sqlFile:
        for stmt in get_statements(sqlFile):
            write_log(config, "Executing statement:\n{}".format(stmt))
            
            pre_statement(config, migration, stmt)
            
            with conn.cursor() as cur:
                cur.execute(stmt)
            
            post_statement(config, migration, stmt)
    
    return True
# End run_sql_migration


def run_migration_job(config, migrations, startIx, targetIx, incVal):
    """
    Returns bool
    Executes the migration loop from start to target incrementing positively or negatively 
    depending if the job is an upgrade or downgrade respectively. 
    If start and target are equal, then only that one target migration is performed.
    """
    
    conn = config['conn']
    
    if (startIx < targetIx) and incVal < 0:
        raise MigrationError("ERROR: decrementing value would cause an infinite loop.")
    
    if (startIx > targetIx) and incVal > 0:
        raise MigrationError("ERROR: incrementing value would cause an infinite loop.")
    
    i = 0
    totalMigrations = (abs(startIx - targetIx) + 1)
    
    while(True):
        i += 1

        migration = migrations[startIx]
        try:
            write_log(config, "Executing migration {}/{}: {}".format(i, totalMigrations, migration['version']))
            
            pre_script(config, migration)
            
            if migration['filetype'] == 'py':
                rc = run_python_migration(config, migration)
            else:
                rc = run_sql_migration(config, migration)
            
            post_script(config, migration)
            
        except Exception as e:
            write_log(config, 'EXCEPTION {}:: Running migration {}: {}'.format(type(e).__name__, migration['filename'], e), level=logging.ERROR)
            if config.get('verbose', False):
                traceback.print_exc(file=sys.stderr)
            conn.rollback()
            return False
        else:
            # we ran without exception
            if rc:
                # Script ran A-OK!
                try:
                    addOK = add_migration_record(config, migration, current=1)
                except Exception as e:
                    write_log(config, 'EXCEPTION {}:: Adding migration record for version {}: {}'.format(type(e).__name__, migration['version'], e), level=logging.ERROR)
                    if config.get('verbose', False):
                        traceback.print_exc(file=sys.stderr)
                    conn.rollback()
                    return False
                else:
                    if addOK:
                        conn.commit()
                    else:
                        conn.rollback()
                        return False
                
                if startIx == targetIx:
                    return True
                else:
                    startIx += incVal
            else:
                # We had some sort of non-exception or gracefully handled failure
                conn.rollback()
                return False
    # End processing loop
    
    return True
# End run_migration_job


def get_migrations(config):
    """
    Returns list
    Return a list of migrations. Based on the job type, it will look in the upgrades dir or the downgrades dir.
    Uses glob.glob to obtain the list of SQL and python files.
    File extensions should always be lowercase.
    """
    
    import glob
    
    migrationsDir = 'migration_upgrade_dir'if config['migration_action'] == 'upgrade' else 'migration_downgrade_dir'
    migrationsDir = config[migrationsDir]
    
    # only sql and py files allowed!
    migrations = glob.glob(os.path.join(migrationsDir, '*.sql'))
    migrations.extend(glob.glob(os.path.join(migrationsDir, '*.py')))
    
    return migrations
# End get_migrations


def setup_migrations(config):
    """
    Returns list
    Gets the migration file names, creates migration dicts from the filenames, and sorts them by version.
    See get_migration_filename_info().
    """
    
    migrations = get_migrations(config)
    
    if len(migrations) > 0:
        # info-ize them
        migrations = [get_migration_filename_info(config, fn) for fn in migrations]
        # and sort 'em
        migrations = sort_migrations(config, migrations)

    return migrations
# End setup_migrations


def find_migration_file_version(config, migrations, version, prior=False):
    """
    Returns int (or None on failure)
    Finds the target migration version by its version string (not sortable version) in the list of migrations
    """
    
    # find target
    ix = None
    for ix, m in enumerate(migrations):
        if m['version'] == version:
            return ix
    
    if prior:
        return ix
    
    return None
# End find_migration_file_version


def run_upgrade(config):
    """
    Action function. Returns int. This return code will be forwarded to the shell.
    Handles action necessary to run an upgrade migration. Each migration script is run in its own transaction. 
    After each script completes, a migration record is added for the migration script and the transaction is committed.
    """
    
    conn = config['conn']
    targetVersion = config['version']
    
    write_log(config, "Running Upgrade Migrations")
    
    currentVersion = get_current(config)
    baselineVersion = get_baseline(config)
    conn.rollback() # Clear any potential open transactions
    
    if baselineVersion:
        baselineVersion['sort_version'] = get_sort_version(config, baselineVersion['version'])
    
    migrations = setup_migrations(config)
    if not migrations:
        write_log(config, 'There are no upgrade migration files.')
        return 29
    
    if config['version'] == LATEST_VERSION:
        targetIx = len(migrations) - 1
        targetVersion = migrations[targetIx]['version']
    else:
        targetIx = find_migration_file_version(config, migrations, targetVersion)
    if targetIx is None:
        write_log(config, "ERROR:: Could not find target migration version '{}'".format(config['version']), level=logging.ERROR)
        return 20
    
    if config.get('sequential', True) and currentVersion:
        startIx = find_migration_file_version(config, migrations, currentVersion['version'])
        if startIx is None:
            # This could happen if the files don't match the db.
            # set it like a force
            startIx = targetIx
        elif startIx == targetIx: # Sanity check!
            write_log(config, "INFO: Database schema is already at this version (current = {}; target = {})".format(currentVersion['version'], targetVersion))
            return 0
        else:
            # since we've found where we currently are, we need to inc by 1 to get the "real" starting point
            startIx += 1
    elif config.get('sequential', True): # Handle edge case of baseline not set
        startIx = 0
    else:
        startIx = targetIx
    
    if startIx > targetIx:
        write_log(config, "ERROR:: When migrating to an earlier version, you must use --downgrade (current = {}; target = {})".format(currentVersion['version'], targetVersion), level=logging.ERROR)
        return 21
    
    if baselineVersion and (migrations[targetIx]['sort_version'] < baselineVersion['sort_version']):
        write_log(config, "ERROR:: The target version is behind the baseline version (target = {}; baseline = {})".format(targetVersion, baselineVersion['version']), level=logging.ERROR)
        return 22
    
    if baselineVersion and (migrations[startIx]['sort_version'] < baselineVersion['sort_version']):
        write_log(config, "ERROR:: The starting version is behind the baseline version (current = {}; baseline = {})".format(migrations[startIx]['version'], baselineVersion['version']), level=logging.ERROR)
        return 22
    
    try:
        rc = run_migration_job(config, migrations, startIx, targetIx, 1)
    except Exception as e:
        write_log(config, "EXCEPTION {}:: running migration job: {}".format(type(e).__name__, e), level=logging.ERROR)
        return 23
    else:
        if not rc:
            return 24
    
    return 0
# End run_migration_job


def run_downgrade(config):
    """
    Action function. Returns int. This return code will be forwarded to the shell.
    Handles action necessary to run a downgrade migration. Each migration script is run in its own transaction. 
    After each script completes, a migration record is added for the migration script and the transaction is committed.
    """
    
    conn = config['conn']
    
    write_log(config, "Running Downgrade Migrations")
    
    currentVersion = get_current(config)
    baselineVersion = get_baseline(config)
    conn.rollback() # Clear any potential open transactions
    
    migrations = setup_migrations(config)
    if not migrations:
        write_log(config, 'There are no downgrade migration files.')
        return 39
    
    if baselineVersion:
        baselineVersion['sort_version'] = get_sort_version(config, baselineVersion['version'])
    
    targetIx = find_migration_file_version(config, migrations, config['version'])
    if targetIx is None:
        write_log(config, "ERROR:: Could not find target migration version '{}'".format(config['version']), level=logging.ERROR)
        return 30
    
    if config.get('sequential', True) and currentVersion:
        startIx = find_migration_file_version(config, migrations, currentVersion['version'])
        if startIx is None:
            startIx = find_migration_file_version(config, migrations, currentVersion['version'], prior=True)
            if startIx is not None:
                startIx += 1    # This will allow the decrement to put us where we want to be
        
        if startIx is None:
            startIx = targetIx
        elif startIx == targetIx: # Sanity check!
            write_log(config, "INFO: Database schema is already at this version (current = {}; target = {})".format(currentVersion['version'], config['version']))
            return 0
        else:
            # since we've found where we currently are, we need to inc by 1 to get the "real" starting point
            startIx -= 1
    elif not currentVersion:
        write_log(config, "No upgrades have been run. Downgrade not possible.", level=logging.WARNING)
        return 0
    else:
        startIx = targetIx
    
    if startIx < targetIx:
        write_log(config, "ERROR:: When migrating to a later version, you must use --upgrade (current = {}; target = {})".format(currentVersion['version'], config['version']), level=logging.ERROR)
        return 31
    
    if baselineVersion and (migrations[targetIx]['sort_version'] < baselineVersion['sort_version']):
        write_log(config, "ERROR:: The target version is behind the baseline version (target = {}; baseline = {})".format(config['version'], baselineVersion['version']), level=logging.ERROR)
        return 32
    
    if baselineVersion and (migrations[startIx]['sort_version'] < baselineVersion['sort_version']):
        write_log(config, "ERROR:: The starting version is behind the baseline version (current = {}; baseline = {})".format(migrations[startIx]['version'], baselineVersion['version']), level=logging.ERROR)
        return 32
    
    try:
        rc = run_migration_job(config, migrations, startIx, targetIx, -1)
    except Exception as e:
        write_log(config, "EXCEPTION {}:: running migration job: {}".format(type(e).__name__, e), level=logging.ERROR)
        return 34
    else:
        if not rc:
            return 35
    
    return 0
# End run_migration_job


def display_current_version(currentVersion):
    """
    Pretty-prints the dict for the migration record of the current version.
    """
    
    print("Current version:")
    for k in VALID_COLUMNS:
        print("    {}: {}".format(k, currentVersion[k]))
# End display_current_version


def get_info(config):
    """
    Action function. Returns int. 
    Gets and displays the migration record for the current version
    """
    
    write_log(config, "Running Get Info for current version")
    
    currentVersion = get_current(config)
    if currentVersion:
        display_current_version(currentVersion)
    else:
        write_log(config, "No current version information. Migrations baseline may not have been set yet.", level=logging.ERROR)
    
    return 0
# End get_info


def verify_version(config):
    """
    Action function. Returns int (zero (0) is success). 
    Gets the migration record for the current version and compares the version string against the target version string.
    """
    
    write_log(config, "Running Verify Version = {}".format(config['version']))
    
    currentVersion = get_current(config)
    if currentVersion:
        if currentVersion['version'] == config['version']:
            rc = 0
        else:
            write_log(config, "Version mismatch! Current = {}; Target = {}.".format(currentVersion['version'], config['version']))
            rc = 50
    else:
        write_log(config, "No current version information. Migrations baseline may not have been set yet.", level=logging.ERROR)
        rc = 51
    
    return rc
# End get_info


def wrap_text(text, limit):
    """
    Wrap a text string at a certain width (limit)
    """
    
    s = e = None
    for step in range(0, len(text), limit):
        if s is None:
            s = step
        else:
            e = step
            yield text[s:e]
            s = e
    # End loop
    yield text[s:]
# End wrap_text


def write_header(fields, lengths):
    """
    Write migration data output header
    """
    
    fmtStr = ' | '.join("{{{0}:{1}s}}".format(i, lengths[fields[i]]) for i in range(len(fields)))
    print(fmtStr.format(*fields))
    
    fmtStr = '-+-'.join("{{{0}:-<{1}s}}".format(i, lengths[fields[i]]) for i in range(len(fields)))
    print(fmtStr.format(*[''] * len(fields)))
# End write_header


def write_line(record, fields, lengths):
    """
    Write a formatted table of data to stdout.
    """
    
    fmtStr = ' | '.join("{{{0}:{1}s}}".format(col, lengths[col]) for col in fields)
    tmp = record.copy()
    out = dict.fromkeys(fields, '')
    
    # Wrap all the things
    for col in fields:
        tmp[col] = {i: t for i, t in enumerate(wrap_text(str(tmp[col]), lengths[col]))}
    
    # output wraped stuffs
    i = 0
    while True:
        haveData = False
        for col in fields:
            out[col] = tmp[col].get(i, '')
            if not haveData:
                haveData = bool(out[col])
        
        if haveData:
            print(fmtStr.format(**out))
            i += 1
        else:
            break
    # End output loop
# End write_line


def output_migration_data(config, migrationLog):
    """
    Print formatted migration data to stdout
    """
    
    fields = ['#'] + VALID_COLUMNS
    lengths = dict(zip(fields, [5] + COLUMN_LENGTHS))
    write_header(fields, lengths)
    for i, record in enumerate(migrationLog):
        record['#'] = i
        write_line(record, fields, lengths)
    # End record loop
# End get_migration_data


def get_migration_data(config):
    """
    Get all migration data from database migration table
    """
    
    conn = config['conn']
    
    try:
        with conn.cursor() as cur:
            cur.execute("""select * from {}{} order by applied_ts;""".format(config.get('migration_table_schema', ''), config['migration_table_name']))
            res = cur.fetchall()
    except Exception as e:
        write_log(config, "EXCEPTION:: Getting migration application log ({}).".format(e), level=logging.ERROR)
        if config.get('verbose', False):
            traceback.print_exc(file=sys.stderr)
        return None
    finally:
        conn.rollback()
    
    return res
# End get_migration_data


def migration_log(config):
    """
    Print the migration log
    """
    
    data = get_migration_data(config)
    if data is None:
        return 60
    elif len(data) == 0:
        write_log(config, "No migration data is available")
    else:
        output_migration_data(config, data)
    
    return 0
# End dump_migrations


def new_config():
    """
    Create a new config dictionary
    """
    
    return dict()
# End new_config


def initialize(configFileName, action, version, sequential=True, verbose=False):
    """
    Perform all initializations for pydbvolve:
        Load config file
        Create config dict
        Confirm directories
        Setup log
        Get DB credentials
        Get DB connection
    """
    
    write_log({}, "Loading config code from '{}'".format(configFileName))
    load_config(configFileName)
    
    config = new_config()
    config.update({'migration_action': action, 
                   'version': version,
                   'migration_user': get_migration_user(config),
                   'sequential': sequential,
                   'verbose': verbose})
    
    config.update(get_config())
    
    confirm_dirs(config)
    
    setup_log(config)
    write_log(config, "Running {} as user {}".format(os.path.basename(sys.argv[0]), config['migration_user']))
    
    write_log(config, "Getting DB Credentials")
    try:
        credentials = get_db_credentials(config)
    except Exception as e:
        write_log(config, "EXCEPTION:: Getting database credentials: {}".format(e), level=logging.ERROR)
        return None
    
    if credentials:
        config['db_user'] = get_database_user(config, credentials)
        write_log(config, "Getting DB connection")
        try:
            config['conn'] = get_db_connection(config, credentials)
        except Exception as e:
            write_log(config, "EXCEPTION:: Getting database connection: {}".format(e), level=logging.ERROR)
            return None
        finally:
            del credentials
    else:
        write_log(config, "Failed to get DB credentials", level=logging.ERROR)
    
    return config
# End initialize


def run_migration(configFileName, action, version, sequential, verbose=False):
    """
    Main handler function for pydbvolve. 
    If you intend to import pydbvolve into a larger project, this is the function that should serve as the entry point.
    Handles: 
        Initialization
        Verification of migrations table
        Resolve action argument to action function
        Execute action function
    """
    
    if not os.access(configFileName, os.F_OK | os.R_OK):
        write_log({}, "Config file '{}' does not exist or cannot be read.".format(configFileName), level=logging.ERROR)
        return 1
    
    config = initialize(configFileName, action, version, sequential, verbose)
    if not config:
        write_log({}, "Error creating config dict. Script cannot run.", level=logging.ERROR)
        return 2
    if not config.get('conn'):
        write_log(config, "Could not get a database connection. Please verify your credentials and connectivity.", level=logging.ERROR)
        return 3
    
    # Verify action code
    if action not in VALID_ACTIONS:
        write_log(config, "ERROR:: action must be one of {}".format(', '.join(sorted(VALID_ACTIONS))), level=logging.ERROR)
        return 4
    
    # Resolve action code string to action function
    if action == 'baseline':
        action = set_baseline
    elif action == 'upgrade':
        action = run_upgrade
    elif action == 'downgrade':
        if version == LATEST_VERSION:
            write_log(config, "Cannot downgrade to 'latest'", level=logging.ERROR)
            return 5  # re-using this one since it's still an action check
        action = run_downgrade
    elif action == 'info':
        action = get_info
    elif action == 'log':
        action = migration_log
    elif action == 'verify':
        action = verify_version
    else:
        write_log(config, "Unknown action {}. Exiting.".format(action), level=logging.ERROR)
        return 5
    
    # Verify migration table
    write_log(config, "Checking for migrations table")
    try:
        migrateTableExists = check_migration_table(config)
    except Exception as e:
        write_log(config, "EXCEPTION {}:: Error with migrations table: {}".format(type(e).__name__, e), level=logging.ERROR)
        return 6
    if not migrateTableExists:
        create_migration_table(config)
    
    # Perform action
    try:
        pre_execution(config)
    except Exception as e:
        write_log(config, "EXCEPTION performing pre-execution", level=logging.ERROR)
        if config.get('verbose', False):
            traceback.print_exc(file=sys.stderr)
        rc = 7
    else:
        try:
            rc = action(config)
        except Exception as e:
            write_log(config, "Error performing action {}".format(action.__name__), level=logging.ERROR)
            if config.get('verbose', False):
                traceback.print_exc(file=sys.stderr)
            rc = 8
        else:
            try:
                post_execution(config)
            except Exception as e:
                write_log(config, "EXCEPTION performing post-execution", level=logging.ERROR)
                if config.get('verbose', False):
                    traceback.print_exc(file=sys.stderr)
                rc = 9
        finally:
            if config.get('conn'):
                write_log(config, "Closing database connection")
                config['conn'].close()
            close_log(config)
    
    return rc
# End run_migration

