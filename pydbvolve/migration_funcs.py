# migrations

import uuid
import os
import re
import traceback
import importlib
import importlib.machinery as ilmac
import importlib.util as ilutil
from datetime import datetime as dt
from .global_settings import VALID_COLUMNS
from .exceptions import *


def get_migration_filename(config, migration):
    path = config['migration_upgrade_dir'] if config['migration_action'] == 'upgrade' else config['migration_downgrade_dir']
    return os.path.join(path, migration['migration_file'])
# End get_migration_filename_info


def get_baseline(config):
    """
    Returns a dict representing the baseline migration record or empty dict if none exist.
    """
    
    conn = config['conn']
    write_log(config, "Getting baseline version")
    try:
        with conn.cursor() as cur:
            cur.execute("""select * from {}"{}" where is_baseline = 1 and is_active = 1""".format(config.get('migration_table_schema', ''), config['migration_table_name']))
            res = cur.fetchone()
    except Exception as e:
        write_log(config, 'EXCEPTION:: getting baseline version {}'.format(e), level=logging.ERROR)
        raise exception_code(e, 100)
    else:
        if res is None:
            res = {}
    
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
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    except Exception as e:
        write_log(config, "EXCEPTION:: reset of baseline flag failed! {}\n Stmt:\n{}".format(e, sql), level=logging.ERROR)
        raise exception_code(e, 100)
    else:
        return True
# End clear_baseline


def get_current(config):
    """
    Returns a dict representing the baseline migration record or empty dict if none exist.
    """
    
    conn = config['conn']
    try:
        write_log(config, "Getting current migration")
        with conn.cursor() as cur:
            cur.execute("""select * from {}"{}" where is_current = 1 and is_active = 1""".format(config.get('migration_table_schema', ''), config['migration_table_name']))
            res = cur.fetchone()
    except Exception as e:
        msg = 'EXCEPTION:: getting current migration {}'.format(e)
        write_log(config, msg, level=logging.ERROR)
        raise exception_code(e, 100)
    else:
        if res is None:
            res = {}
    
    return res
# End get_current


def find_current(config):
    conn = config['conn']
    sql = """
select * 
  from {}"{}"
 where requested_ts is not null
   and applied_ts is not null
 order 
    by applied_ts desc;
""".format(config.get('migration_table_schema', ''), config['migration_table_name'])
    
    withh conn.cursor() as cur:
        cur.execute(sql)
        res = cur.fetchone()
    
    return res
# End find_curent


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
# End clear_current


def reset_current(config):
    conn = config['conn']
    sql = """
update {}"{}"
   set is_current = 1
 where id = {};
""".format(config.get('migration_table_schema', ''), config['migration_table_name'], config['positional_variable_marker'])
    
    clear_current(config)
    rec = find_current(config)
    if rec:
        with conn.cursor() as cur:
            cur.execute(sql, (rec['id'],))
# End reset_current


def lock_migration_table(config):
    conn = config['conn']
    stmt = config['lock_table_tmpl'].format(config['migration_table_name'])
    with conn.cursor() as cur():
        cur.execute(stmt)
# End lock_migration_table


def update_migration_record(config, migration):
    """
    updates a migration record
    """
    
    if migration['is_current'] == 1:
        clear_current(config)
    if migration['is_baseline'] == 1:
        clear_baseline(config)
    
    pos_var = config['positional_variable_marker']
    
    conn = config['conn']
    sql = """
update {}"{}"
   set {}
 where id = {};
""".format(config.get('migration_table_schema', ''), 
           config['migration_table_name'], 
           ', '.join("{} = {}".format(c, pos_var) for c in VALID_COLUMNS))
    valuesd = dict.fromkeys(VALID_COLUMNS)
    for k in VALID_COLUMNS:
        valuesd[k] = config.get(k)
    
    valuesd['is_current'] = current
    valuesd['is_baseline'] = baseline
    valuesd['applied_ts'] = applied
    valuesd['requested_ts'] = requested
    valuesd['migration_file'] = os.path.basename(migration.get('filename', ''))[:256]
    valuesd['migration_type'] = migration.get('filetype', '')
    valuesd['version'] = migration.get('version', config['version'])
    valuesd['is_active'] = active
    valuesd['id'] = migration.get('id')
    
    with conn.cursor() as cur:
        cur.execute(sql, valuesd)
# End update_migration_record


def add_migration_record(config, migration, requested, current=0, baseline=0, active=1, applied=None):
    """
    Adds a migration record to the migrations table. 
    If it is a baseline record, the existing baseline will be unset. 
    If it is a current record, the existing current will be unset.
    Returns bool
    """
    
    if current == 1:
        clear_current(config)
    if baseline == 1:
        clear_baseline(config)
    if current == 0 and baseline == 0:
        msg = "ERROR:: The flags 'current' and 'baseline' cannot both be zero (0)"
        write_log(config, msg, level=logging.ERROR)
        raise MigrationTableManagementError(msg)
    if not isinstance(requested, dt):
        msg = "ERROR:: The requested parameter must be a datetime"
        write_log(config, msg, level=logging.ERROR)
        raise MigrationTableManagementError(msg)
    if not isinstance(applied, (dt, type(None)):
        msg = "ERROR:: The applied parameter must be a datetime or None"
        write_log(config, msg, level=logging.ERROR)
        raise MigrationTableManagementError(msg)
        
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
    valuesd['applied_ts'] = applied
    valuesd['requested_ts'] = requested
    valuesd['migration_file'] = os.path.basename(migration.get('filename', ''))[:256]
    valuesd['migration_type'] = migration.get('filetype', '')
    valuesd['version'] = migration.get('version', config['version'])
    valuesd['is_active'] = active
    valuesd['id'] = get_migration_id(config)
    
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


def create_migration(config, filename):
    
    
    filename = os.path.basename(filename)
    upfilepath = os.path.join(config['migration_upgrade_dir'], filename)
    downfilepath = os.path.join(config['migration_downgrade_dir'], filename)
    
    if not os.path.exists(upfilepath):
        open(upfilepath, 'w')
    if not os.path.exists(downfilepath):
        open(downfilepath, 'w')
    
    
    





def set_baseline(config):
    """
    Set a baseline record in the database. 
    The baseline is the absolute marker for the past. 
    You cannot downgrade past the baseline. 
    You cannot upgrade to a target before the start or before the baseline.
    """
    
    conn = config['conn']
# End set_baseline


def generate_migration_table_sql(config):
    schema = config.get('migration_table_schema', '')
    tableName = config['migration_table_name']
    sql = """
create table {0}"{1}"
(
    id               varchar(75) primary key -- 'nuff said?
    requested_ts     {2} not null,           -- time version was successfully applied
    applied_ts       {2},                    -- time version was successfully applied
    migration_file   varchar(256) not null,  -- file name of migration run
    migration_action varchar(256) not null,  -- 'upgrade', 'downgrade', 'quueued', etc
    migration_type   varchar(256) not null,  -- 'python', 'sql', ...
    migration_description varchar(256),
    migration_user   varchar(256) not null,  -- name of user running migration program
    db_user          varchar(256) not null,  -- name of database user applying sql statements
    is_current       integer not null,       -- flag to denote current version (for convenience)
    is_baseline      integer not null,       -- flag for baseline version
    is_active        integer not null        -- flag for active migration or not
);
""".format(schema, tableName, config['timestamp_type'])
    
    return sql
# End generate_migration_table_sql


def generate_migration_table_index_sql(config):
    schema = config.get('migration_table_schema', '')
    tableName = config['migration_table_name']
    
    indexes = [
        """create unique index ux01__migrations__ on {}"{}" (version, applied_ts);""".format(schema, tableName),
        """create unique index ux02__migrations__ on {}"{}" (version, requested_ts);""".format(schema, tableName),
        """create index ix01__migrations__ on {}"{}" (is_current);""".format(schema, tableName),
        """create index ix02__migrations__ on {}"{}" (is_baseline);""".format(schema, tableName)
    ]
    
    return indexes
# End generate_migration_table_index_sql


def drop_migration_table(config):
    with config['conn'].cursor() as cur:
        cur.execute('drop table {}"{}";'.format(config.get('migration_table_schema', ''), 
                                                config['migration_table_name']))
# End drop_migration_table


def export_migration_table(config):
    export_file_name = config['migration_id']
    config['_export_ts'] = dt.now()
    if 'migration_table_schema' not in config:
        config['migration_table_schema'] = ''
    
    with open(export_file_name, 'w') as exp_file:
        print('-- export migration table\nUser: {migration_user}\nDate: {_export_ts}\n\n '\
              'drop table {migration_table_schema}"{migration_table_name}"{sql_statement_seq}\n'.format(**config), file=exp_file)
        print(generate_migration_table_sql(config), file=exp_file)
        for ix in generate_migration_table_index_sql(config):
            print(ix, file=exp_file)
        
        
    


def create_migration_table(config):
    """
    Creates the migration table in the specified schema.
    """
    
    conn = config['conn']
    sql = generate_migration_table_sql(config)
    indexes = generate_migration_table_index_sql(config)
    
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


def import_any(fileName, modName):
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
# End import_any


def get_migration_record(config, mig_id):
    conn = config['conn']
    sql = """
select * from {}"{}" where id = {};
""".format(config.get('migration_table_schema', ''), config['migration_table_name'], config['positional_variable_marker'])
    
    with conn.cursor() as cur:
        cur.execute(sql, (mig_id,))
        res = cur.fetchone()
    
    return res
# End get_migration_record


def get_migration_record_using_file(config, filename):
    conn = config['conn']
    filename = os.path.basename(filename)
    
    sql = """
select * 
  from {}"{}"
 where requested_ts is not null
   and is_active = 1
   and migration_file = {};
""".format(config.get('migration_table_schema', ''), config['migration_table_name'], config['positional_variable_marker'])
    
    with conn.cursor() as cur:
        cur.execute(sql, (filename,))
        res = cur.fetchall()
    
    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]
    else:
        msg = "There are multiple files in the migration log table that have the same name and are active. "\
              "There should be at most only 1 active record with a given file name"
        raise MigrationTableManagementError(msg)
# End get_migration_record_using_file


def resolve_file_and_id(config):
    try:
        mig_id = uuid.UUID(str(config['file_or_id']))
    except:
        config['migration_file'] = config['file_or_id']
        config['migration_id'] = get_mig_id_from_file(config)
    else:
        rec = get_migration_record(config, str(mig_id))
        if rec:
            config['migration_id'] = rec['id']
            config['migration_file'] = rec['migration_file']
        else:
            rec = get_migration_record_using_file(config, config['file_or_id'])
            if rec:
                config['migration_id'] = rec['id']
                config['migration_file'] = rec['migration_file']
            else:
                config['migration_id'] = None
                config['migration_file'] = config['file_or_id']
# End resolve_file_and_id


def verify_file_type(config):
    if not config['migration_file'].lower().endswith('.py') and not config['migration_file'].lower().endswith('.sql'):
        raise MigrationFileTypeError("Only SQL (.sql) or Python3 ('.py') files can be processed")
# End verify_file_type
    

def display_version_info(version_info, legend):
    """
    Pretty-prints the dict for the migration record of the current version.
    """
    
    print(legend)
    width = max(len(c) for c in VALID_COLUMNS)
    for k in VALID_COLUMNS:
        print("    {0:{1}s} : {2}".format(k, width, version_info[k]))
# End display_current_version


def get_info(config):
    """
    Action function. Returns int. 
    Gets and displays the migration record for the current version
    """

    pass
# End get_info


def verify_migration(config):
    """
    Action function. Returns int (zero (0) is success). 
    Gets the migration record for the current version and compares the version string against the target version string.
    """
    
    write_log(config, "Running Verify Version = {}".format(config['version']))
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


def write_header(tmp_file, fields, lengths):
    """
    Write migration data output header
    """
    
    fmtStr = ' | '.join("{{{0}:{1}s}}".format(i, lengths[fields[i]]) for i in range(len(fields)))
    print(fmtStr.format(*fields), file=tmp_file)
    
    fmtStr = '-+-'.join("{{{0}:-<{1}s}}".format(i, lengths[fields[i]]) for i in range(len(fields)))
    print(fmtStr.format(*[''] * len(fields)), file=tmp_file)
# End write_header


def write_line(tmp_file, record, fields, lengths):
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
            print(fmtStr.format(**out), file=tmp_file)
            i += 1
        else:
            break
    # End output loop
# End write_line


def output_migration_data(config, migrationLog):
    """
    Print formatted migration data to stdout
    """
    import tempfile
    import subprocess
    import shutil
    
    tmp_file, tmp_file_name = tempfile.mkstemp(text=True)
    tmp_file = os.fdopen(tmp_file, 'w')
    fields = ['#'] + VALID_COLUMNS
    lengths = dict(zip(fields, [5] + COLUMN_LENGTHS))
    write_header(tmp_file, fields, lengths)
    
    if bool(shutil.which('less')):
        traversal = reversed(range(len(migrationLog)))
    else:
        traversal = range(len(migrationLog))
    
    for i in traversal:
        record = migrationLog[i]
        record['#'] = i
        write_line(tmp_file, record, fields, lengths)
    # End record loop
    
    tmp_file.flush()
    tmp_file.close()
    
    try:
        subprocess.call(['less', '-E' '-F', '-P', ':', tmp_file_name])
    except:
        with open(tmp_file_name, 'r') as tmpf:
            buff = ['']
            while len(buff):
                print(buff)
                buff = tmpf.read(4000)
    finally:
        os.unlink(tmp_file_name)
# End output_migration_data


def get_migration_data(config):
    """
    Get all migration data from database migration table
    """
    
    conn = config['conn']
    sql = """
select * 
  from {}"{}" 
 order 
    by coalesce(applied_ts, requested_ts);
""".format(config.get('migration_table_schema', ''), 
           config['migration_table_name'])
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
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
    if not data:
        write_log(config, "No migration data is available")
    else:
        output_migration_data(config, data)
    
    return NO_ERROR
# End dump_migrations


def get_unapplied_migrations(config):
    pass
# End get_unapplied_migrations


def run_python_migration(config, migration):
    """
    Returns bool.
    Loads and runs a Python migration as a module. 
    The Python migration module should contain a function named 'run_config' 
    that accepts two positional arguments for the config dict and the migration dict.
    """
    
    # Expose the write_log func to the migration
    config['write_log'] = write_log
    
    # Expost pre/post statement JIC
    config['pre_statement'] = pre_statement
    config['post_statement'] = post_statement
    
    # Expose the base migration exception to the migration
    config['migration_exception'] = MigrationError
    
    write_log(config, 'Loading python migration "{}"'.format(migration['filename']))
    migration_module_name = 'pv_mg_' + migration['version']
    pymigration = import_any(migration['filename'], migration_module_name)
    
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


def get_statements(config, sqlFile):
    """
    Returns str
    Reads 1-MiB chunks of a sql file and parses statements from each chunk. 
    Each statement found is yielded so this function is a generator.
    SQL migration statements are delimited by get_sql_statement_sep().
    """
    
    readLimit = 1000000
    stmtSep = config['sql_statement_sep']
    
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
                post_statement(config, migration, stmt, cur)
    
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
    migration_type = 'downgrade' if incVal < 0 else 'upgrade'
    
    while(True):
        i += 1

        migration = migrations[startIx]
        try:
            # For comfort's sake, we're going to now be chatty here.
            msg = "Executing {} migration {}/{}: {}".format(migration_type, 
                                                            i, 
                                                            totalMigrations, 
                                                            os.path.basename(migration['filename']))
            if config.get('chatty'):
                print(msg)
            write_log(config, msg)
            
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
    
    conn = config['conn']
    if config['migration_action'] == 'upgrade':
        sql = """
select * from {}"{}" where applied_ts is null and is_active = 1 order by requested_ts;
""".format(config['migration_table_schema'], config['migration_table_name'])
    else:
        sql = """
select * from {}"{}" where applied_ts is not null and is_active = 1 order by applied_ts desc;
""".format(config['migration_table_schema'], config['migration_table_name'])
    
    with conn.cursor() as cur:
        cur.execute(sql)
        migrations = cur.fetchall()
    
    return migrations
# End get_migrations


