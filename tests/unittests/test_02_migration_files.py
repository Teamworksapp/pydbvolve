import os
import sys
import sqlite3

import os
import sys

# Set path to force the import of the local module
sys.path.insert(1, os.path.abspath('.'))
import pydbvolve

TEST_CONFIG_FILE = os.path.join('tests', 'pydbvolve.conf')


def test_00_local_module(capsys):
    """Verify that we are using the local module."""
    with capsys.disabled():
        #print(pydbvolve.__path__)
        assert('site-packages' not in '|'.join(pydbvolve.__path__))
        assert('dist-packages' not in '|'.join(pydbvolve.__path__))
# End test_00_local_module


def test_01_get_migrations():
    """Verify that migration files can be retrieved in the upgrade directory and downgrade directory"""
    # Get migrations from upgrade dir
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.10', True, False)
    migrations = pydbvolve.get_migrations(config)
    assert(len(migrations) > 0)
    assert(all('remove' not in m for m in migrations))
    # Get migrations from downgrade dir
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'downgrade', 'r1.1.10', True, False)
    migrations = pydbvolve.get_migrations(config)
    assert(len(migrations) > 0)
    assert(any('remove' in m for m in migrations))
# End test_01_get_migrations


def test_02_get_migration_filename_info():
    """Verify that filename information can be generated from the migration filenames"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    info = pydbvolve.get_migration_filename_info(config, '')
    assert(info is None)
    migrations = pydbvolve.get_migrations(config)
    info = pydbvolve.get_migration_filename_info(config, migrations[0])
    assert(info is not None)
    assert(all(k in info for k in ('version', 'description', 'filetype', 'filename', 'sort_version')))
    assert(all(bool(info[k]) for k in ('version', 'description', 'filetype', 'filename', 'sort_version')))
    assert(isinstance(info['sort_version'], tuple))
    assert(all(isinstance(v, int) for v in info['sort_version']))
# End test_02_get_migration_filename_info


def test_03_sort_migrations():
    """Verify that the migration file list can be sorted properly by the sort_version."""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    migrations = pydbvolve.setup_migrations(config)
    lastSort = (-1, -1, -1)
    for migration in migrations:
        assert(lastSort < migration['sort_version'])
        lastSort = migration['sort_version']
# End test_03_sort_migrations


def test_04_find_migration_string():
    """Verify that a particular migration can be found by the version string match"""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.0.0')
    assert(isinstance(res, int))
    assert(res > -1)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r99.99.999')
    assert(res is None)
# End test_04_find_migration_string


def test_05_parse_sql_statements():
    """Verify that a sql migration file's statements can be parsed by the statement separator."""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.1.0', True, False)
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.0.0')
    assert(isinstance(res, int))
    assert(res > -1)
    sqlfile = open(migrations[res]['filename'], 'r')
    sep = pydbvolve.get_sql_statement_sep()
    assert(sep.search('-- run') is not None)
    statements = [stmt for stmt in pydbvolve.get_statements(sqlfile)]
    sqlfile.close()
    assert(len(statements) == 6)
    assert('create table person' in statements[0])
    assert('unique index ix01_person' in statements[1])
    assert('ix02_person' in statements[2])
    assert('ix03_person' in statements[3])
    assert('create table school' in statements[4])
    assert('insert into school' in statements[5])
# End test_05_parse_sql_statements


def test_06_load_python_migration():
    """Verify that a python migration can be loaded into a module reference."""
    config = pydbvolve.initialize(TEST_CONFIG_FILE, 'upgrade', 'r1.2.0', True, False)
    migrations = pydbvolve.setup_migrations(config)
    res = pydbvolve.find_migration_file_version(config, migrations, 'r1.2.0')
    assert(isinstance(res, int))
    assert(res > -1)
    migration = migrations[res]
    pymigration = pydbvolve.import_arbitrary(migration['filename'], 'pv_mg_' + migration['version'])
    elements = set(dir(pymigration))
    assert('create_user_table' in elements)
    assert('add_default_user' in elements)
    assert('run_migration' in elements)
# End test_06_load_python_migration


