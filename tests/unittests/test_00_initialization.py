import os
import sys
import sqlite3

import os
import sys
import importlib

# Set path to force the import of the local module
sys.path.insert(1, os.path.abspath('.'))
import pydbvolve

TEST_CONFIG_FILE = os.path.join('tests', 'pydbvolve.conf')
TEST_DB_FILE = os.path.join('tests', 'test_db.sqlite')


def test_00_local_module(capsys):
    """Verify that we are using the local module."""
    with capsys.disabled():
        #print(pydbvolve.__path__)
        assert('site-packages' not in '|'.join(pydbvolve.__path__))
        assert('dist-packages' not in '|'.join(pydbvolve.__path__))
# End test_00_local_module


def test_01_new_config():
    """Verify that the return from new_config call is an instance of dict"""
    assert(isinstance(pydbvolve.new_config(), dict))
# End test_new_config


def test_02_get_migration_user():
    """Verify that the get_migration_user returns a non-empty string"""
    muser = pydbvolve.get_migration_user({})
    assert(muser is not None)
    assert(isinstance(muser, str))
    assert(len(muser) > 0)
# End test_get_migration_user


def test_03_get_base_dir():
    """Verify that we get a non-Falsey return from get_base_dir."""
    bdir = pydbvolve.get_base_dir()
    assert(bdir is not None)
    assert(isinstance(bdir, str))
    assert(len(bdir) > 0)
# End test_03_get_base_dir


def test_04_get_migration_base_dir():
    """Verify that we get a non-Falsey return from get_migration_base_dir."""
    mdir = pydbvolve.get_migration_base_dir()
    assert(mdir is not None)
    assert(isinstance(mdir, str))
    assert(len(mdir) > 0)
    assert(mdir.startswith(pydbvolve.get_base_dir()))
# End test_04_get_migration_base_dir


def test_05_get_migration_upgrade_dir():
    """Verify that we get a non-Falsey return from get_migration_upgrade_dir."""
    mdir = pydbvolve.get_migration_upgrade_dir()
    assert(mdir is not None)
    assert(isinstance(mdir, str))
    assert(len(mdir) > 0)
    assert(mdir.startswith(pydbvolve.get_migration_base_dir()))
# End test_05_get_migration_upgrade_dir


def test_06_get_migration_downgrade_dir():
    """Verify that we get a non-Falsey return from get_migration_downgrade_dir."""
    mdir = pydbvolve.get_migration_downgrade_dir()
    assert(mdir is not None)
    assert(isinstance(mdir, str))
    assert(len(mdir) > 0)
    assert(mdir.startswith(pydbvolve.get_migration_base_dir()))
# End test_06_get_migration_downgrade_dir


def test_07_get_log_dir():
    """Verify that we get a non-Falsey return from get_log_dir."""
    mdir = pydbvolve.get_log_dir()
    assert(mdir is not None)
    assert(isinstance(mdir, str))
    assert(len(mdir) > 0)
    assert(mdir.startswith(pydbvolve.get_base_dir()))
# End test_07_get_log_dir


def test_08_get_migration_table_name():
    """Verify that we get a non-Falsey return from get_migration_table_name."""
    table_name = pydbvolve.get_migration_table_name()
    assert(table_name is not None)
    assert(isinstance(table_name, str))
    assert(len(table_name) > 0)
# End test_08_get_migration_table_name


def test_09_get_migration_table_schema():
    """Verify that we get a non-Falsey return from get_migration_table_schema."""
    schema = pydbvolve.get_migration_table_schema()
    assert(schema is not None)
    assert(isinstance(schema, str))
    assert(len(schema) > 0)
# End test_09_get_migration_table_name


def test_10_get_positional_variable_marker():
    """Verify that we get back a non-Falsey return from get_positional_variable_marker."""
    marker = pydbvolve.get_positional_variable_marker()
    assert(marker is not None)
    assert(isinstance(marker, str))
    assert(len(marker) > 0)
# End test_10_get_positional_variable_marker


def test_11_log_file(capsys):
    """Verify that we can open a log file."""
    with capsys.disabled():
        config = {'log_file_name': 'tmp.log',
                  'logger_name': 'f',
                  'version': 'r1.0.0',
                  'migration_action': 'info'}
        pydbvolve.setup_file_logger(config)
        f = config['logger']
        config['log_file_name'] = None
        config['logger_name'] = 's'
        pydbvolve.setup_stream_logger(config)
        s = config['logger']
        assert(f is not None)
        assert(s is not None)
        assert(f.__class__.__name__ == 'Logger')
        assert(s.__class__.__name__ == 'Logger')
        assert(f.name == 'f')
        assert(s.name == 's')
        assert(len(s.handlers) == 1)
        assert(len(f.handlers) >= 1)
# End test_11_log_file


def test_12_get_filename_regex():
    """Verify that we get a compiled filename regex from get_filename_regex."""
    import re
    
    r = pydbvolve.get_filename_regex()
    assert(r is not None)
    assert(r.__class__.__name__ == 'SRE_Pattern')
# End test_12_get_filename_regex


def test_13_pre_config():
    res = None
    
    try:
        res = pydbvolve.get_db_credentials(None)
    except Exception as e:
        assert(e.__class__.__name__ == 'NotImplementedError')
    
    assert(res is None)
    
    try:
        res = pydbvolve.get_db_connection(None, None)
    except Exception as f:
        assert(f.__class__.__name__ == 'NotImplementedError')
    
    assert(res is None)
# End test_13_pre_config


def test_14_load_config():
    """Load the test config file and verify that it overrides the speficied functions."""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    old_get_db_credentials = pydbvolve.get_db_credentials
    old_get_base_dir = pydbvolve.get_base_dir
    old_get_db_connection = pydbvolve.get_db_connection
    old_get_positional_variable_marker = pydbvolve.get_positional_variable_marker
    old_get_migration_table_schema = pydbvolve.get_migration_table_schema
    
    pydbvolve.load_config(TEST_CONFIG_FILE)
    
    assert(old_get_db_credentials != pydbvolve.get_db_credentials)
    assert(old_get_base_dir != pydbvolve.get_base_dir)
    assert(old_get_db_connection != pydbvolve.get_db_connection)
    assert(old_get_positional_variable_marker != pydbvolve.get_positional_variable_marker)
    assert(old_get_migration_table_schema != pydbvolve.get_migration_table_schema)
# End test_load_config


def test_15_get_config():
    """Verify we get a config dict with all expected keys present and having no None values."""
    def get_migration_table_schema():
        return "public"
    
    pydbvolve.load_config(TEST_CONFIG_FILE)
    pydbvolve.get_migration_table_schema = get_migration_table_schema
    config = pydbvolve.new_config()
    config.update({'migration_action': 'info', 
                   'version': pydbvolve.LATEST_VERSION,
                   'migration_user': pydbvolve.get_migration_user(config),
                   'sequential': False,
                   'verbose': False})
    config.update(pydbvolve.get_config())
    
    for key in ('base_dir',
                'migration_dir',
                'migration_upgrade_dir',
                'migration_downgrade_dir',
                'filename_regex',
                'log_dir',
                'migration_table_schema',
                'migration_table_name',
                'positional_variable_marker',
                'migration_action',
                'version',
                'migration_user',
                'sequential',
                'verbose'):
        assert(key in config)
        assert(config[key] is not None)
    
    importlib.reload(pydbvolve)
# End test_15_get_config


def test_16_confirm_dirs():
    pydbvolve.load_config(TEST_CONFIG_FILE)
    config = pydbvolve.new_config()
    config.update({'migration_action': 'info', 
                   'version': pydbvolve.LATEST_VERSION,
                   'migration_user': pydbvolve.get_migration_user(config),
                   'sequential': False,
                   'verbose': False})
    config.update(pydbvolve.get_config())
    pydbvolve.confirm_dirs(config)
    
    for dirname in (k for k in config.keys() if k.endswith('_dir')):
        assert(os.path.exists(config[dirname]))
# End test_16_confirm_dirs


def test_17_get_db_credentials():
    """Test the return from get_db_credentials call"""
    pydbvolve.load_config(TEST_CONFIG_FILE)
    config = pydbvolve.new_config()
    config.update(pydbvolve.get_config())
    cred = pydbvolve.get_db_credentials(config)
    
    assert(cred is not None)
    assert(isinstance(cred, dict))
    assert(len(cred) > 0)
# End test_17_get_db_credentials


def test_18_get_database_user():
    pydbvolve.load_config(TEST_CONFIG_FILE)
    config = pydbvolve.new_config()
    config.update(pydbvolve.get_config())
    cred = pydbvolve.get_db_credentials(config)
    dbuser = pydbvolve.get_database_user(config, cred)
    
    assert(dbuser is not None)
    assert(isinstance(dbuser, str))
    assert(len(dbuser) > 0)
# End test_18_get_database_user


def test_19_connect_db():
    """Verify that we can get a connection based on the definition of the get_db_connection config function."""
    pydbvolve.load_config(TEST_CONFIG_FILE)
    config = pydbvolve.new_config()
    config.update({'migration_action': 'info', 
                   'version': pydbvolve.LATEST_VERSION,
                   'migration_user': pydbvolve.get_migration_user(config),
                   'sequential': False,
                   'verbose': False})
    config.update(pydbvolve.get_config())
    cred = pydbvolve.get_db_credentials(config)
    conn = pydbvolve.get_db_connection(config, cred)
    
    assert(conn is not None)
    assert(isinstance(conn, sqlite3.Connection))
    
    conn.close()
# End test_19_connect_db


def test_20_initialize(capsys):
    """Verify top-level call to component functions."""
    with capsys.disabled():
        config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
        assert(config is not None)
        assert(isinstance(config, dict))
        assert(len(config) > 0)
        assert(config['base_dir'] == './tests')
        assert(config['migration_action'] == 'info')
        assert(config['version'] == 'r1.1.10')
        assert(config['sequential'] == True)
        assert(config['verbose'] == False)
        assert(config['migration_table_name'] == '__migrations__')
        assert(isinstance(config['conn'], sqlite3.Connection))
        assert('credentials' not in config)
        
        config['conn'].close()
# End test_20_initialize


def test_21_get_sql_statement_sep():
    """Verify that we get a compiled sql statement separator regex from get_sql_statement_sep."""
    import re
    
    r = pydbvolve.get_sql_statement_sep()
    assert(r is not None)
    assert(r.__class__.__name__ == 'SRE_Pattern')
# End test_21_get_sql_statement_sep


def test_22_set_log_file_name():
    """Verify that we get a formatted log file name from the config."""
    import datetime
    
    pydbvolve.load_config(TEST_CONFIG_FILE)
    config = pydbvolve.new_config()
    config.update(pydbvolve.get_config())
    config['version'] = 'r1.0.0'
    config['migration_action'] = 'upgrade'
    
    pydbvolve.set_log_file_name(config)
    file_name = config['log_file_name']
    
    assert(file_name is not None)
    assert(file_name != '')
    assert(config['migration_action'].replace(' ', '_') in file_name)
    assert(config['version'].replace(' ', '_') in file_name)
    assert(str(datetime.datetime.now().date()) in file_name)
# End test_21_get_sql_statement_sep


