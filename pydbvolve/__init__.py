# ====================================
#  All recurds returned by database calls should be of dict type or behave exactly like dict!!
# ====================================

__VERSION__ = (1, 1, 0)
__VERSION_STRING__ = '.'.join(str(v) for v in __VERSION__)

# Uncomment for debugging
# try:
#     from IPython import embed
# except:
#     pass

import sys
import os
import traceback


from .trigger_funcs import *
from .logging_funcs import *
from .config_funcs import *
from .migration_funcs import *
from .exceptions import *


def initialize(config_file_name, action, file_or_id, sequential=True, dry_run=False, verbose=False, chatty=False):
    """
    Perform all initializations for pydbvolve:
        Load config file
        Create config dict
        Confirm directories
        Setup log
        Get DB credentials
        Get DB connection
    """
    
    write_log({}, "Loading config code from '{}'".format(config_file_name))
    load_config_file(config_file_name)
    
    config = new_config()
    config.update({'migration_action': action, 
                   'file_or_id': file_of_id,
                   'migration_user': get_migration_user(config),
                   'sequential': sequential,
                   'dry_run': dry_run,
                   'verbose': verbose,
                   'chatty': chatty,
                   'config_file_path': os.path.abspath(config_file_name)})
    
    # get_config calls the config setup functions that may be overridden by the config code
    run_config(config)
    
    confirm_dirs(config)
    
    setup_log(config)
    msg = "Running {} as user {}".format(os.path.basename(sys.argv[0]), config['migration_user'])
    if chatty:
        print(msg)
    write_log(config, msg)
    
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


def run_migration(config_file_name, action, file_or_id, dry_run=False, verbose=False, chatty=False):
    """
    Main handler function for pydbvolve. 
    If you intend to import pydbvolve into a larger project, this is the function that should serve as the entry point.
    Handles: 
        Initialization
        Verification of migrations table
        Resolve action argument to action function
        Execute action function
    """
    
    if not os.access(config_file_name, os.F_OK | os.R_OK):
        write_log({}, "Config file '{}' does not exist or cannot be read.".format(config_file_name), level=logging.ERROR)
        return 1
    
    config = initialize(config_file_name, action, file_or_id, sequential, dry_run, verbose, chatty)
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
        action = run_downgrade
    elif action == 'info':
        action = get_info
    elif action == 'log':
        action = migration_log
    elif action == 'verify':
        action = verify_version
    elif action == 'create':
        action = create_migration
    elif action == 'delete':
        action = delete_migration
    elif action == 'touch':
        action = touch_migration
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
        write_log(config, "EXCEPTION performing pre-execution: {}".format(e), level=logging.ERROR)
        if config.get('verbose', False):
            traceback.print_exc(file=sys.stderr)
        rc = getattr(e, 'exc_code', -1)
    else:
        try:
            rc = action(config)
        except Exception as e:
            write_log(config, "Error performing action {}: {}".format(action.__name__, e), level=logging.ERROR)
            if config.get('verbose', False):
                traceback.print_exc(file=sys.stderr)
            rc = getattr(e, 'exc_code', -1)
        else:
            try:
                post_execution(config)
            except Exception as e:
                write_log(config, "EXCEPTION performing post-execution: {}".format(e), level=logging.ERROR)
                if config.get('verbose', False):
                    traceback.print_exc(file=sys.stderr)
                rc = getattr(e, 'exc_code', -1)
        finally:
            if config.get('conn'):
                write_log(config, "Closing database connection")
                config['conn'].close()
            close_log(config)
    
    return rc
# End run_migration

