# config

import re
import traceback

from .exceptions import (MigrationTableOutOfSync, MigrationTableConstraintError)


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


def get_base_dir(config_file_path):
    """
    Returns the base directory path. Default is './pydbvolve'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(os.path.dirname(config_file_path), 'pydbvolve')
# End get_base_dir


def get_migration_base_dir(base_dir):
    """
    Returns the base directory for the migrations. Default is get_base_dir() + '/migrations'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(base_dir, "migrations")
# End get_migration_dir


def get_migration_upgrade_dir(migration_base_dir):
    """
    Returns the base directory for the migrations. Default is get_migration_base_dir() + '/upgrades'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(migration_base_dir, "upgrades")
# End get_migration_upgrade_dir


def get_migration_downgrade_dir(migration_base_dir):
    """
    Returns the base directory for the migrations. Default is get_migration_base_dir() + '/downgrades'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(migration_base_dir, "downgrades")
# End get_migration_downgrade_dir


def get_log_dir(base_dir):
    """
    Returns the base directory for the migrations. Default is get_base_dir() + '/logs'.
    Overide this function in your config file to set a custom directory.
    """
    
    return os.path.join(base_dir, "logs")
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
    Returns the string used to set a positional variable in a sql statement buffer for your driver. Default is "%s".
    Overide this function if your driver does not use "%s" as a positional variable indicator.
    """
    
    return '%s'
# End get_positional_variable_marker


def get_timestamp_datatype_name():
    """
    Returns the data type name for timestamps (date and time stored in the same datatype). Default is "timestamp".
    Overide this function in your config file to set your databases timestamp datatype.
    """
    
    return 'timestamp'
# End get_timestamp_datatype_name


def get_table_lock_statement_template():
    """
    Returns the table lock statement to use to control access to the migration log/driver table. 
    Default is (must use '{}' as a table placeholder) "lock table {} in access exclusive mode nowait;"
    This is used to prevent multiple pydbvolve calls running at the same time.
    override this function to 
    """
    
    return "lock table {} in access exclusive mode nowait;"


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


def get_sql_statement_sep():
    """
    Returns SQL statement separator regex.
    Default is '--run' on its own line.
    """
    
    return re.compile('^\s*--\s*run\s*$', flags=re.MULTILINE|re.IGNORECASE)
# End get_sql_statement_sep


def run_config(config):
    """
    Build a config dict from the loaded config Python file.
    """
    
    schema = get_migration_table_schema()
    if schema:
        schema = '"{}".'.format(schema)
    
    base_dir = get_base_dir(config['config_file_path'])
    migration_dir = get_migration_base_dir(base_dir)
    
    config.update({
        'base_dir': base_dir,
        'migration_dir': migration_dir,
        'migration_upgrade_dir': get_migration_upgrade_dir(migration_dir),
        'migration_downgrade_dir': get_migration_downgrade_dir(migration_dir),
        'log_dir': get_log_dir(base_dir),
        'migration_table_schema': schema,
        'migration_table_name': get_migration_table_name(),
        'positional_variable_marker': get_positional_variable_marker(),
        'timestamp_type': get_timestamp_datatype_name(),
        'lock_table_tmpl': get_table_lock_statement_template(),
        'sql_statement_sep': get_sql_statement_sep()
    })
    
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


def load_config_file(configFileName):
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


def new_config():
    """
    Create a new config dictionary
    """
    
    return dict()
# End new_config


