import sqlite3
import os
import sys
import importlib
import re
from io import StringIO

# Set path to force the import of the local module
sys.path.insert(1, os.path.abspath('.'))
import pydbvolve

TEST_CONFIG_FILE = os.path.join('tests', 'pydbvolve.conf')
TEST_DB_FILE = os.path.join('tests', 'test_db.sqlite')
TEST_OUT_FILE = os.path.join('tests', 'test_post_statement.txt')


def _table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""select * from sqlite_master where tbl_name = ?;""", (table_name,))
        res = cur.fetchone()
    return res is not None
# End _table_exists


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


def test_00_local_module(capsys):
    """Verify that we are using the local module."""
    with capsys.disabled():
        #print(pydbvolve.__path__)
        assert('site-packages' not in '|'.join(pydbvolve.__path__))
        assert('dist-packages' not in '|'.join(pydbvolve.__path__))
# End test_00_local_module


def test_01_run_sql_migration():
    """Verify that a sql migration can be run successfully"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.0.0', True, False)
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.0.0')
    migration = migrations[res]
    rc = pydbvolve.run_sql_migration(config, migration)
    
    assert(rc)
    assert(_table_exists(config['conn'], 'person'))
    assert(_table_exists(config['conn'], 'school'))
    config['conn'].close()
    
    os.unlink(TEST_DB_FILE)
# End test_01_run_sql_migration


def test_02_run_python_migration():
    """Verify that a sql migration can be run successfully"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.2.0', True, False)
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.2.0')
    migration = migrations[res]
    
    assert(migration['filetype'] == 'py')
    
    rc = pydbvolve.run_python_migration(config, migration)
    
    assert(rc)
    assert(_table_exists(config['conn'], 'users'))
    
    sql = """
select count(*) as "count" from "users";
"""
    with config['conn'].cursor() as cur:
        cur.execute(sql)
        res = cur.fetchone()['count']
    
    assert(res == 1)
    config['conn'].close()
    
    os.unlink(TEST_DB_FILE)
# End test_01_run_sql_migration


def test_03_run_migration_job_not_serial():
    """Verify that a migration can be run and a migration record created"""
    def pre_script(config, migration):
        raise Exception("Force a condition")
    def post_script(config, migration):
        _drop_migration_table(config)
    
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.0.0', True, False)
    migrateTableExists = pydbvolve.check_migration_table(config)
    assert(not migrateTableExists)
    if not migrateTableExists:
        pydbvolve.create_migration_table(config)
    
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.0.0')
    migration = migrations[res]
    
    exc = None
    try:
        rc = pydbvolve.run_migration_job(config, migrations, res, res - 1, 1)
    except Exception as e:
        exc = e
    
    assert(isinstance(exc, pydbvolve.MigrationError))
    
    exc = None
    try:
        rc = pydbvolve.run_migration_job(config, migrations, res - 1, res, -1)
    except Exception as e:
        exc = e
    
    assert(isinstance(exc, pydbvolve.MigrationError))
    
    rc = pydbvolve.run_migration_job(config, migrations, res, res, 1)
    assert(rc)
    
    curr = pydbvolve.get_current(config)
    assert(curr['version'] == 'r1.0.0')
    
    save = pydbvolve.pre_script
    pydbvolve.pre_script = pre_script
    rc = pydbvolve.run_migration_job(config, migrations, res+1, res+1, 1)
    assert(rc == False)
    pydbvolve.pre_script = save
    save = pydbvolve.post_script
    pydbvolve.post_script = post_script
    rc = pydbvolve.run_migration_job(config, migrations, res+1, res+1, 1)
    assert(rc == False)
    pydbvolve.post_script = save
    
    os.unlink(TEST_DB_FILE)
# End test_03_run_migration_jobs


def test_04_run_migration_job_serial():
    """Verify that a migrations can be applied serially"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    migrateTableExists = pydbvolve.check_migration_table(config)
    assert(not migrateTableExists)
    if not migrateTableExists:
        pydbvolve.create_migration_table(config)
    
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.1.0')
    migration = migrations[res]
    
    rc = pydbvolve.run_migration_job(config, migrations, 0, res, 1)
    assert(rc)
    
    curr = pydbvolve.get_current(config)
    assert(curr['version'] == 'r1.1.0')
    
    all_migrations = pydbvolve.get_migration_data(config)
    assert(all_migrations[0]['version'] == 'r0.0.0')
    assert(all_migrations[0]['is_current'] == False)
    assert(all_migrations[1]['version'] == 'r1.0.0')
    assert(all_migrations[1]['is_current'] == False)
    assert(all_migrations[2]['version'] == 'r1.1.0')
    assert(all_migrations[2]['is_current'] == True)
    
    _drop_migration_table(config)
    all_migrations = pydbvolve.get_migration_data(config)
    assert(all_migrations is None)
    
    os.unlink(TEST_DB_FILE)
# End test_03_run_migration_jobs


def test_05_run_downgrade_migration_job_serial():
    """Verify that downgrade migrations can be applied serially"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.2.0', True, False)
    migrateTableExists = pydbvolve.check_migration_table(config)
    assert(not migrateTableExists)
    if not migrateTableExists:
        pydbvolve.create_migration_table(config)
    
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.2.0')
    rc = pydbvolve.run_migration_job(config, migrations, 0, res, 1)
    assert(rc)
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'downgrade', 'r1.0.0', True, False)
    currentVersion = pydbvolve.get_current(config)
    migrations = pydbvolve.setup_migrations(config)
    start = pydbvolve.find_migration_file_version(config, migrations, currentVersion['version'])
    start -= 1
    target = pydbvolve.find_migration_file_version(config, migrations, 'r1.0.0')
    rc = pydbvolve.run_migration_job(config, migrations, start, target, -1)
    assert(rc)
    
    curr = pydbvolve.get_current(config)
    assert(curr['version'] == 'r1.0.0')
    
    all_migrations = pydbvolve.get_migration_data(config)
    assert(all_migrations[0]['version'] == 'r0.0.0')
    assert(all_migrations[0]['is_current'] == False)
    assert(all_migrations[1]['version'] == 'r1.0.0')
    assert(all_migrations[1]['is_current'] == False)
    assert(all_migrations[2]['version'] == 'r1.1.0')
    assert(all_migrations[2]['is_current'] == False)
    assert(all_migrations[3]['version'] == 'r1.2.0')
    assert(all_migrations[3]['is_current'] == False)
    assert(all_migrations[4]['version'] == 'r1.1.0')
    assert(all_migrations[4]['is_current'] == False)
    assert(all_migrations[5]['version'] == 'r1.0.0')
    assert(all_migrations[5]['is_current'] == True)
    
    os.unlink(TEST_DB_FILE)
# End test_05_run_downgrade_migration_job_serial


def test_06_set_baseline():
    """Verify set_baseline functions"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'baseline', 'r0.0.0', True, False)
    migrateTableExists = pydbvolve.check_migration_table(config)
    assert(not migrateTableExists)
    if not migrateTableExists:
        pydbvolve.create_migration_table(config)
    
    rc = pydbvolve.set_baseline(config)
    assert(rc == 0)
    
    baseline = pydbvolve.get_baseline(config)
    assert(baseline['version'] == 'r0.0.0')
    assert(baseline['is_baseline'] == True)
    
    _drop_migration_table(config)
    rc = pydbvolve.set_baseline(config)
    assert(rc != 0)
    
    os.unlink(TEST_DB_FILE)
# End test_06_set_baseline


def test_07_run_upgrade():
    """Verify set_baseline functions"""
    def no_migrations(*args, **kwargs):
        return []
    
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'baseline', 'r1.1.0', True, False)
    migrateTableExists = pydbvolve.check_migration_table(config)
    assert(not migrateTableExists)
    if not migrateTableExists:
        pydbvolve.create_migration_table(config)
    
    rc = pydbvolve.set_baseline(config)
    assert(rc == 0)
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.2.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc == 0)
    config['conn'].close()
    
    # cannot upgrade to an earlier version than current
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc != 0)
    config['conn'].close()

    # can upgrade to later version
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.3.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc == 0)

    all_migrations = pydbvolve.get_migration_data(config)
    all_versions = set(m['version'] for m in all_migrations)
    assert('r1.2.0' in all_versions)
    assert('r1.2.9' in all_versions)
    assert('r1.3.0' in all_versions)
    assert('r1.3.1' not in all_versions)
    
    save = pydbvolve.setup_migrations
    pydbvolve.setup_migrations = no_migrations
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.3.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc != 0)
    
    pydbvolve.setup_migrations = save
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.3.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc == 0)
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'baseline', 'r1.2.0', True, False)
    rc = pydbvolve.set_baseline(config)
    assert(rc == 0)
    
    pydbvolve.setup_migrations = no_migrations
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.0.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc != 0)
    
    config['conn'].close()
    
    importlib.reload(pydbvolve)
    
    os.unlink(TEST_DB_FILE)
# End test_07_set_baseline


def test_08_run_downgrade():
    """Verify set_baseline functions"""
    def no_migrations(*args, **kwargs):
        return []
    
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'baseline', 'r1.2.0', True, False)
    migrateTableExists = pydbvolve.check_migration_table(config)
    assert(not migrateTableExists)
    if not migrateTableExists:
        pydbvolve.create_migration_table(config)
    
    pydbvolve.set_baseline(config)
    config['conn'].close()
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.3.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc == 0)
    config['conn'].close()
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'downgrade', 'r1.2.0', True, False)
    rc = pydbvolve.run_downgrade(config)
    assert(rc == 0)
    
    curr = pydbvolve.get_current(config)
    assert(curr['version'] == 'r1.2.0')
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'downgrade', 'r1.2.0', True, False)
    rc = pydbvolve.run_downgrade(config)
    assert(rc == 0)
    
    save = pydbvolve.setup_migrations
    pydbvolve.setup_migrations = no_migrations
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'downgrade', 'r1.1.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc != 0)
    
    pydbvolve.setup_migrations = save
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'downgrade', 'r1.1.0', True, False)
    rc = pydbvolve.run_upgrade(config)
    assert(rc != 0)
    
    config['conn'].close()
    
    importlib.reload(pydbvolve)
    
    os.unlink(TEST_DB_FILE)
# End test_08_run_downgrade


def test_09_verify_version():
    """Verify set_baseline functions"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.3.0', True, False)
    migrateTableExists = pydbvolve.check_migration_table(config)
    assert(not migrateTableExists)
    if not migrateTableExists:
        pydbvolve.create_migration_table(config)
    
    pydbvolve.set_baseline(config)
    config['conn'].close()
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'verify', 'r1.3.0', True, False)
    rc = pydbvolve.verify_version(config)
    assert(rc == 0)
    
    config['version'] = 'r1.1.1'
    rc = pydbvolve.verify_version(config)
    assert(rc != 0)
    
    pydbvolve.clear_current(config)
    rc = pydbvolve.verify_version(config)
    assert(rc != 0)
    
    config['conn'].close()
    
    os.unlink(TEST_DB_FILE)
# End test_08_run_downgrade


def test_10_run_migration_bad_migration_table():
    """Verify run_migration exectuion detects bad structure"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, False)
    assert(rc == 0)
    
    conn = sqlite3.connect(TEST_DB_FILE)
    conn.execute("drop table __migrations__;")
    conn.execute("""
create table __migrations__ (bad text, cols text);
""")
    conn.close()
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, False)
    assert(rc != 0)
    
    os.unlink(TEST_DB_FILE)
# End test_10_run_migration_bad_migration_table


def test_11_run_migration_bad_command():
    """Verify that run_migration detects a bad command"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'BAD_COMMAND_STRING', 'r1.0.0', True, False)
    assert(rc != 0)
    
    # test second path (which should not be hit, but you never know...)
    pydbvolve.VALID_ACTIONS.add('BAD_COMMAND_STRING')
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'BAD_COMMAND_STRING', 'r1.0.0', True, False)
    assert(rc != 0)
    
    importlib.reload(pydbvolve)
    
    os.unlink(TEST_DB_FILE)
# End test_11_run_migration_bad_command
    

def test_12_run_migration_bad_config():
    """Verify that run_migration detects a bad config"""
    def bad_init_1(*args, **kwargs):
        return None
    def bad_init_2(*args, **kwargs):
        return {'a': 1}
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE + 'bad', 'BAD_COMMAND_STRING', 'r1.0.0', True, False)
    assert(rc != 0)
    
    pydbvolve.initialize = bad_init_1
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'BAD_COMMAND_STRING', 'r1.0.0', True, False)
    assert(rc != 0)
    
    pydbvolve.initialize = bad_init_2
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'BAD_COMMAND_STRING', 'r1.0.0', True, False)
    assert(rc != 0)
    
    importlib.reload(pydbvolve)
# End test_12_run_migration_bad_config
    

def test_13_run_migration(capsys):
    """Verify run_migration exectuion"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.2.0', True, False)
    assert(rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', pydbvolve.LATEST_VERSION, True, False)
    assert(rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'verify', 'r1.3.1', True, False)
    assert(rc == 0)
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'verify', 'r1.3.1', True, False)
    all_migrations = pydbvolve.get_migration_data(config)
    all_versions = set(m['version'] for m in all_migrations)
    assert('r0.0.0' not in all_versions)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'downgrade', 'r1.2.0', True, False)
    assert(rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'verify', 'r1.2.0', True, False)
    assert(rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'downgrade', 'r1.1.0', True, True)
    assert(rc != 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.3.0', True, False)
    assert(rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'downgrade', 'r1.1.0', True, True)
    assert(rc != 0)
    
    os.unlink(TEST_DB_FILE)
# End test_13_run_migration


def test_14_migration_log():
    """Verify migration_log execution"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, False)
    assert(rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', pydbvolve.LATEST_VERSION, True, False)
    assert(rc == 0)
    
    rc = True
    try:
        pydbvolve.run_migration(TEST_CONFIG_FILE, 'log', 'all', True, False)
    except:
        rc = False
    
    assert(rc)
    
    os.unlink(TEST_DB_FILE)
# End test_14_migration_log


def test_15_pre_exec_trap_exception():
    def raise_it(*args, **kwargs):
        raise Exception("Something")
    
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    pydbvolve.pre_execution = raise_it
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, False)
    assert(rc != 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, True)
    assert(rc != 0)
    
    importlib.reload(pydbvolve)
    
    os.unlink(TEST_DB_FILE)
# End test_15_pre_exec_trap_exception


def test_16_post_exec_trap_exception():
    def raise_it(*args, **kwargs):
        raise Exception("Something")
    
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    pydbvolve.post_execution = raise_it
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, False)
    assert(rc != 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, True)
    assert(rc != 0)
    
    importlib.reload(pydbvolve)
    
    os.unlink(TEST_DB_FILE)
# End test_16_post_exec_trap_exception


def test_17_get_info():
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', 'r1.0.0', True, False)
    assert(rc == 0)
    
    rc = True
    try:
        pydbvolve.run_migration(TEST_CONFIG_FILE, 'info', 'current', True, False)
    except:
        rc = False
    
    assert(rc)
    
    os.unlink(TEST_DB_FILE)
# End test_17_get_info
    

def test_18_bad_upgrade_downgrade():
    """Verify run_migration exectuion"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', '100.100.200', True, False)
    assert(rc != 0)

    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'downgrade', '100.100.200', True, False)
    assert(rc != 0)
    
    os.unlink(TEST_DB_FILE)
# End test_18_bad_upgrade


def test_19_upgrade_downgrade_without_baseline():
    """Verify run_migration exectuion"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', pydbvolve.LATEST_VERSION, True, False)
    assert(rc == 0)

    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'downgrade', 'r0.0.0', True, False)
    assert(rc == 0)
    
    os.unlink(TEST_DB_FILE)
# End test_19_upgrade_downgrade_without_baseline


def test_20_downgrade_without_upgrade(capsys):
    """Verify run_migration exectuion"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'downgrade', 'r0.0.0', True, False)
    assert(rc == 30)
    
    os.unlink(TEST_DB_FILE)
# End test_18_bad_upgrade


def test_21_upgrade_baseline_current(capsys):
    """Verify baseline-current and baseline-info and get_version()"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass
    
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    assert (config is not None)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', pydbvolve.LATEST_VERSION, True, False)
    assert (rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', pydbvolve.CURRENT_VERSION, True, False)
    assert (rc == 0)
    
    curr = pydbvolve.get_current(config)
    assert curr is not None
    base = pydbvolve.get_baseline(config)
    assert base is not None
    assert curr['version'] == base['version']
    
    os.unlink(TEST_DB_FILE)
# End test_21_upgrade_baseline_current


def test_22_downgrade_baseline(capsys):
    """Verify downgrade-baseline"""
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass

    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    assert (config is not None)

    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', 'r1.2.9', True, False, chatty=False)
    assert (rc == 0)
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'baseline', pydbvolve.CURRENT_VERSION, True, False)
    assert (rc == 0)
    
    curr = pydbvolve.get_current(config)
    assert curr is not None
    base = pydbvolve.get_current(config)
    assert curr is not None
    assert curr['version'] == 'r1.2.9'
    assert curr['version'] == base['version']

    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', pydbvolve.LATEST_VERSION, True, False)
    assert (rc == 0)

    curr = pydbvolve.get_current(config)
    assert curr is not None
    assert curr['version'] != 'r1.2.9'
    
    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'downgrade', pydbvolve.BASELINE_VERSION, True, False)
    assert (rc == 0)

    curr = pydbvolve.get_current(config)
    assert curr is not None
    assert curr['version'] == 'r1.2.9'

    os.unlink(TEST_DB_FILE)

# End test_21_upgrade_baseline_current


def test_22_post_statement():
    try:
        os.unlink(TEST_DB_FILE)
    except:
        pass

    rc = pydbvolve.run_migration(TEST_CONFIG_FILE, 'upgrade', 'r1.0.0', True, False)
    assert (rc == 0)
    assert ('rowcount' in pydbvolve.TEST_OUT)
    assert (pydbvolve.TEST_OUT['rowcount'] == 1)

    os.unlink(TEST_DB_FILE)
# End test_22_post_statement


