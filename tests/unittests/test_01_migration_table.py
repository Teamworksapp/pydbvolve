import os
import sys
import datetime

import os
import sys

# Set path to force the import of the local module
sys.path.insert(1, os.path.abspath('.'))
import pydbvolve

TEST_CONFIG_FILE = os.path.join('tests', 'pydbvolve.conf')


def _drop_migration_table(config):
    conn = config['conn']
    # remove the table if it exists
    sql = """
select * from sqlite_master where tbl_name = ?;
"""
    with conn.cursor() as cur:
        cur.execute(sql, (config['migration_table_name'], ))
        res = cur.fetchone()
    
    if res:
        sql = '''
drop table {};
'''.format(config['migration_table_name'])
        with config['conn'].cursor() as cur:
            cur.execute(sql)
    
    config['conn'].commit()
# End _drop_migration_table    


def _create_bad_migration_table(config):
    conn = config['conn']
    sql = '''
create table {}"{}" (id integer, shooba text, dooba text);
'''.format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    with conn.cursor() as cur:
        cur.execute(sql)
    
    conn.commit()
# End _create_bad_migration_table


def _empty_migation_table(config):
    conn = config['conn']
    sql = '''
delete from {}"{}";
'''.format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    with conn.cursor() as cur:
        cur.execute(sql)
    
    conn.commit()
# End _empty_migation_table


def _add_migration_record(config, record):
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
""".format(config.get('migration_table_schema', ''), config['migration_table_name'], ', '.join(pydbvolve.VALID_COLUMNS), ', '.join([config['positional_variable_marker']] * len(pydbvolve.VALID_COLUMNS)))
    values = tuple(record.get(c, '') for c in pydbvolve.VALID_COLUMNS)
    
    with conn.cursor() as cur:
        cur.execute(sql, values)
    
    conn.commit()
# End _add_migration_record


def _count_current(config):
    conn = config['conn']
    sql = """
select count(*) as "count" from {}"{}" where is_current = 1;
""".format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    with conn.cursor() as cur:
        cur.execute(sql)
        res = cur.fetchone().get('count')
    
    return res
# End _count_current


def _count_baseline(config):
    conn = config['conn']
    sql = """
select count(*) as "count" from {}"{}" where is_baseline = 1;
""".format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    with conn.cursor() as cur:
        cur.execute(sql)
        res = cur.fetchone().get('count')
    
    return res
# End _count_current


def test_00_local_module(capsys):
    """Verify that we are using the local module."""
    with capsys.disabled():
        #print(pydbvolve.__path__)
        assert('site-packages' not in '|'.join(pydbvolve.__path__))
        assert('dist-packages' not in '|'.join(pydbvolve.__path__))
# End test_00_local_module


def test_01_check_migration_table_init():
    """Verify that the migration table can be detected"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.check_migration_table(config)
    assert(res == False)
# End test_01_check_migration_table_init


def test_02_check_migration_table_bad_structure():
    """Verify that an aberrant table structure can be detected"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _create_bad_migration_table(config)
    exc = None
    try:
        res = pydbvolve.check_migration_table(config)
    except Exception as e:
        exc = e
    
    assert(isinstance(exc, pydbvolve.MigrationTableOutOfSync))
# End test_02_check_migration_table_bad_structure


def test_03_create_migration_table():
    """Verify that the migration table can be created"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res == True)
    assert(pydbvolve.check_migration_table(config))
    res = pydbvolve.create_migration_table(config)
    assert(res == False)
# End test_03_create_migration_table
    

def test_04_check_migration_table_bad_current():
    """Verify that multiple current recurds are detected"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res)
    record = {
        'version': 'r1.0.0',
        'applied_ts': datetime.datetime.now(),
        'migration_file': 'somefile.sql',
        'migration_type': 'sql',
        'migration_user': config['migration_user'],
        'db_user': config['db_user'],
        'is_current': 1,
        'is_baseline': 0
    }
    _add_migration_record(config, record)
    record['version'] = 'r1.0.1'
    _add_migration_record(config, record)
    
    try:
        res = pydbvolve.check_migration_table(config)
    except Exception as e:
        res = False
        assert(isinstance(e, pydbvolve.MigrationTableConstraintError))
    
    assert(res == False)
# End test_04_check_migration_table_bad_current


def test_05_check_migration_table_bad_baseline():
    """Verify that multiple baseline recurds are detected"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res)
    record = {
        'version': 'r1.0.0',
        'applied_ts': datetime.datetime.now(),
        'migration_file': 'somefile.sql',
        'migration_type': 'sql',
        'migration_user': config['migration_user'],
        'db_user': config['db_user'],
        'is_current': 0,
        'is_baseline': 1
    }
    _add_migration_record(config, record)
    record['version'] = 'r1.0.1'
    _add_migration_record(config, record)
    
    try:
        res = pydbvolve.check_migration_table(config)
    except Exception as e:
        res = False
        assert(isinstance(e, pydbvolve.MigrationTableConstraintError))
    
    assert(res == False)
# End test_04_check_migration_table_bad_current


def test_06_clear_current():
    """Verify that the current record flag can be cleared"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res)
    record = {
        'version': 'r1.0.0',
        'applied_ts': datetime.datetime.now(),
        'migration_file': 'somefile.sql',
        'migration_type': 'sql',
        'migration_user': config['migration_user'],
        'db_user': config['db_user'],
        'is_current': 1,
        'is_baseline': 0
    }
    _add_migration_record(config, record)
    assert(_count_current(config) == 1)
    res = pydbvolve.clear_current(config)
    assert(res)
    assert(_count_current(config) == 0)
    config['conn'].commit()
    
    _drop_migration_table(config)
    res = pydbvolve.clear_current(config)
    assert(res == False)
# End test_05_clear_current


def test_07_clear_baseline():
    """Verify that the baseline record flag can be cleared"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res)
    record = {
        'version': 'r1.0.0',
        'applied_ts': datetime.datetime.now(),
        'migration_file': 'somefile.sql',
        'migration_type': 'sql',
        'migration_user': config['migration_user'],
        'db_user': config['db_user'],
        'is_current': 0,
        'is_baseline': 1
    }
    _add_migration_record(config, record)
    assert(_count_baseline(config) == 1)
    res = pydbvolve.clear_baseline(config)
    assert(res)
    assert(_count_baseline(config) == 0)
    config['conn'].commit()
    
    _drop_migration_table(config)
    res = pydbvolve.clear_baseline(config)
    assert(res == False)
# End test_05_clear_baseline


def test_08_get_current():
    """Verify that the current record can be retrieved"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res)
    record = {
        'version': 'r1.0.0',
        'applied_ts': datetime.datetime.now(),
        'migration_file': 'somefile.sql',
        'migration_type': 'sql',
        'migration_user': config['migration_user'],
        'db_user': config['db_user'],
        'is_current': 1,
        'is_baseline': 0
    }
    res = pydbvolve.get_current(config)
    assert(res == {})
    
    _add_migration_record(config, record)
    assert(_count_current(config) == 1)
    res = pydbvolve.get_current(config)
    assert(bool(res))
    assert((res['version'], str(res['applied_ts'])) == (record['version'], str(record['applied_ts'])))
    
    _drop_migration_table(config)
    res = pydbvolve.get_current(config)
    assert(res == {})
# End test_07_get_current


def test_09_get_baseline():
    """Verify that the baseline record can be retrieved"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res)
    record = {
        'version': 'r1.0.0',
        'applied_ts': datetime.datetime.now(),
        'migration_file': 'somefile.sql',
        'migration_type': 'sql',
        'migration_user': config['migration_user'],
        'db_user': config['db_user'],
        'is_current': 0,
        'is_baseline': 1
    }
    res = pydbvolve.get_baseline(config)
    assert(res == {})
    
    _add_migration_record(config, record)
    assert(_count_baseline(config) == 1)
    res = pydbvolve.get_baseline(config)
    assert(bool(res))
    assert((res['version'], str(res['applied_ts'])) == (record['version'], str(record['applied_ts'])))
    
    _drop_migration_table(config)
    res = pydbvolve.get_baseline(config)
    assert(res == {})
# End test_05_clear_baseline


def test_10_add_migration_record():
    """Verify that a migration record can be added"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'info', 'r1.1.10', True, False)
    _drop_migration_table(config)
    res = pydbvolve.create_migration_table(config)
    assert(res)
    migration = {'version': 'r1.0.0',
                 'sort_version': (1, 0, 0),
                 'filename': 'a_test.sql',
                 'filetype': 'sql'}
    res = pydbvolve.add_migration_record(config, migration, 1, 0)
    assert(res)
    assert(_count_current(config) == 1)
    migration['version'] = 'r1.0.1'
    res = pydbvolve.add_migration_record(config, migration, 1, 0)
    assert(res)
    assert(_count_current(config) == 1)
    config['conn'].commit()
    
    res = pydbvolve.add_migration_record(config, migration, 0, 0)
    assert(res == False)
    
    migration['version'] = None
    res = None
    try:
        res = pydbvolve.add_migration_record(config, migration, 1, 0)
    except Exception as e:
        res = e
    assert(isinstance(res, Exception))
    config['conn'].rollback()
    
    _drop_migration_table(config)
    res = pydbvolve.add_migration_record(config, migration, 1, 0)
    assert(res == False)

    res = pydbvolve.add_migration_record(config, migration, 0, 1)
    assert(res == False)
# End test_10_add_migration_record

