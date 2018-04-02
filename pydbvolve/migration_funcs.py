# migrations

import uuid
import re
import traceback
import importlib
import importlib.machinery as ilmac
import importlib.util as ilutil
from datetime import datetime as dt
from .global_settings import VALID_COLUMNS
from .exceptions import MigrationError


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
        info['filetype'] = info['filetype'].lower()
        info['filename'] = fileName
        info['sort_version'] = get_sort_version(config, info['version'])
        return info
    else:
        return None
# End get_migration_filename_info


def get_sort_version(config, version):
    """
    Returns a form of the version obtained from the execution of get_migration_filename_info() call that can be properly sorted.
    """
    
    noAlpha = re.compile('[^0-9.]+')
    return tuple(int(x) for x in noAlpha.sub('', version).split('.'))
# End get_sort_version


def sort_migrations(config, migrations, reverse=False):
    """
    Returns a sorted version of the migrations list
    """
    
    migrations.sort(key=lambda x: x['sort_version'], reverse=reverse)
    return migrations
# End sort_migrations


def get_baseline(config):
    """
    Returns a dict representing the baseline migration record or empty dict if none exist.
    """
    
    conn = config['conn']
    try:
        write_log(config, "Getting baseline version")
        with conn.cursor() as cur:
            cur.execute("""select * from {}"{}" where is_baseline = 1 and is_active = 1""".format(config.get('migration_table_schema', ''), config['migration_table_name']))
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


def get_version(config, version, exclude_baseline=True):
    """
    Return a dict corresponding to a specific version.   
    """
    
    conn = config['conn']
    try:
        write_log(config, "Getting migration record for version {}".format(version))
        with conn.cursor() as cur:
            sql = """
select * 
  from {}"{}"
 where version = {}
   {}
 order 
    by applied_ts desc;
""".format(config.get('migration_table_schema', ''), 
           config['migration_table_name'],
           config['positional_variable_marker'],
           "and migration_action != 'baseline'" if exclude_baseline else '')
            
            cur.execute(sql, (version,))
            res = cur.fetchone()
            if res is None:
                res = {}
    except Exception as e:
        write_log(config, 'EXCEPTION:: getting version {}: {}'.format(version, e), level=logging.ERROR)
        return {}

    return res
# End get_version


def get_current(config):
    """
    Returns a dict representing the baseline migration record or empty dict if none exist.
    """
    
    conn = config['conn']
    try:
        write_log(config, "Getting current version")
        with conn.cursor() as cur:
            cur.execute("""select * from {}"{}" where is_current = 1 and is_active = 1""".format(config.get('migration_table_schema', ''), config['migration_table_name']))
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


def lock_migration_table(config):
	conn = config['conn']
	stmt = config['lock_table_tmpl'].format(config['migration_table_name'])
	with conn.cursor() as cur():
		cur.execute(stmt)
# End lock_migration_table


def get_max_migration_pk(config):
	conn = config['conn']
	stmt = """
select max(id) as "max_id" from {};
"""
	res = None
	with conn.cursor() as cur:
		cur.execute(stmt)
		res = cur.fetchone()
		if res:
			res = res['max_id']
	
	return res
# End get_max_migration_pk


def get_next_migration_pk(config):
	max_pk = get_max_migration_pk(config) or 0
	return max_pk + 1
# End get_next_migration_pk


def update_migration_record(config, migration, current=0, baseline=0, active=1):
	"""
	updates a migration record
	"""
	
    if current == 1:
        if not clear_current(config):
            return False
    if baseline == 1:
        if not clear_baseline(config):
            return False
	
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


def add_migration_record(config, migration, current=0, baseline=0, active=1, requested=None, applied=None):
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
    if not isinstance(requested, dt) and not isinstance(applied, dt):
        write_log(config, "ERROR:: The requested and applied datetime cannot both be None", level=logging.ERROR)
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
	valuesd['applied_ts'] = applied
	valuesd['requested_ts'] = requested
    valuesd['migration_file'] = os.path.basename(migration.get('filename', ''))[:256]
    valuesd['migration_type'] = migration.get('filetype', '')
    valuesd['version'] = migration.get('version', config['version'])
    valuesd['is_active'] = active
    valuesd['id'] = get_next_migration_pk(config)
    
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
        if config['version'] == CURRENT_VERSION:
            current = get_current(config)
            if not (bool(current)):
                write_log(config, "No current version is set. Have migrations been run?", level=logging.ERROR)
                return 12
            else:
                config['version'] = current['version']
        
        if bool(baseline) and (baseline['version'] == config['version']):
            msg = "Baseline version '{}' has already been set".format(config['version'])
            if config.get('chatty'):
                print(msg)
            write_log(config, msg)
            conn.rollback()
            return 0    # Exit with no error
        else:
            msg = "Setting baseline at version {}".format(config['version'])
            if config.get('chatty'):
                print(msg)
            write_log(config, msg)
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


def generate_migration_table_sql(config):
    schema = config.get('migration_table_schema', '')
    tableName = config['migration_table_name']
    sql = """
create table {0}"{1}"
(
	id               integer primary key     -- 'nuff said?
    migration_id     varchar(256) not null,  -- version string
    requested_ts     {2} not null,           -- time version was successfully applied
    applied_ts       {2} not null,           -- time version was successfully applied
    migration_file   varchar(256) not null,  -- file name of migration run
    migration_action varchar(256) not null,  -- 'upgrade', 'downgrade', etc
    migration_type   varchar(256) not null,  -- 'python', 'sql', ...
    migration_description varchar(256),
    migration_user   varchar(256) not null,  -- name of user running migration program
    db_user          varchar(256) not null,  -- name of database user applying sql statements
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

    baselineVersion = get_baseline(config)

    if config['version'] == CURRENT_VERSION:
        write_log(config, "Running Get Info for current version")
        currentVersion = get_current(config)
        if currentVersion:
            if baselineVersion  and (currentVersion['version'] == baselineVersion['version']):
                currentVersion['is_baseline'] = 1

            display_version_info(currentVersion, "Current version:")
        else:
            write_log(config, "No current version information. Migrations may not have been run yet.", level=logging.WARNING)
    elif config['version'] == BASELINE_VERSION:
        write_log(config, "Running Get Info for baseline version")
        if baselineVersion:
            actualVersion = get_version(config, baselineVersion['version'])
            if actualVersion:
                baselineVersion['migration_file'] = actualVersion['migration_file']
                baselineVersion['migration_action'] = actualVersion['migration_action'] 
                baselineVersion['migration_type'] = actualVersion['migration_type']
                baselineVersion['is_current'] = actualVersion['is_current']
            
            display_version_info(baselineVersion, "Baseline version:")
        else:
            write_log(config, "No baeline version information. Migrations baseline may not have been set yet.",
                      level=logging.WARNING)
    else:
        version = get_version(config, config['version'])
        if not version:
            version = get_version(config, config['version'], exclude_baseline=False)
        if not version:
            write_log(config, "No version information can be found for {}.".format(config['version']),
                      level=logging.WARNING)
        else:
            display_version_info(version, "Version information:")
            

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
    
    try:
        with conn.cursor() as cur:
            sql = """select * from {}{} order by applied_ts;""".format(config.get('migration_table_schema', ''), 
                                                                            config['migration_table_name'])
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
    if data is None:
        return 60
    elif len(data) == 0:
        write_log(config, "No migration data is available")
    else:
        output_migration_data(config, data)
    
    return 0
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
select * from {}"{}" where requested_ts is not null and applied_ts is null and is_active = 1 order by requested_ts;
""".format(config['migration_table_schema'], config['migration_table_name'])
	else:
		sql = """
select * from {}"{}" where requested_ts is null and applied_ts is not null and is_active = 1 order by applied_ts desc;
""".format(config['migration_table_schema'], config['migration_table_name'])
	
	with conn.cursor() as cur:
		cur.execute(sql)
		migrations = cur.fetchall()
    
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
        # Special case check here in case the local files are out-of-sync with the db
        if currentVersion and (get_sort_version(config, targetVersion) < get_sort_version(config, currentVersion['version'])):
            msg = "The database is currently at version {} which is ahead of your latest " \
                  "migration file version {}. Changes will not be made. Your migrations " \
                  "are out-of-sync.".format(currentVersion['version'], targetVersion)
            write_log(config, msg, level=logging.WARNING)
            
            # Exit on no error as if it was a current/target match
            # Yes, I know this is different than when you specify a version directly
            return 0 
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
            write_log(config, "WARNING: Migration file versions are out of sync with the database migration table. This migration will be forced.")
            startIx = targetIx
        elif startIx == targetIx: # Sanity check!
            msg = "INFO: Database schema is already at this version (current = {}; target = {})".format(currentVersion['version'], targetVersion)
            if config.get('chatty'):
                print(msg)
            write_log(config, msg)
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

    currentVersion = get_current(config)
    if currentVersion:
        msg = "Current database version is: {}".format(currentVersion['version'])
        if config.get('chatty'):
            print(msg)
        write_log(config, msg)

    return 0
# End run_upgrade


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
    targetVersion = config['version']
    
    if not currentVersion:
        write_log(config, "No current version. Nothing to downgrade from.", level=logging.WARNING)
        return 30
    
    migrations = setup_migrations(config)
    if not migrations:
        write_log(config, 'There are no downgrade migration files.')
        return 39
    
    if baselineVersion:
        baselineVersion['sort_version'] = get_sort_version(config, baselineVersion['version'])
    
    if config['version'] == BASELINE_VERSION:
        if not baselineVersion:
            write_log(config, 
                      "ERROR:: Cannot downgrade to baseline because no baseline has been set.")
            return 30
        
        targetIx = find_migration_file_version(config, migrations, baselineVersion['version'])
        targetVersion = baselineVersion['version']
        if targetIx is None:
            write_log(config, 
                      "Could not find baseline version {} file! Your migration files are out-of-sync with the database".format(targetVersion),
                      level=logging.ERROR)
            return 31
        elif currentVersion and (get_sort_version(config, baselineVersion['version']) >= get_sort_version(config, currentVersion['version'])):
            msg = "Your current version {} is below the recorded baseline version. " \
                  "Cannot downgrade without resetting baseline.".format(currentVersion['version'], baselineVersion['version'])
            write_log(config, msg, level=logging.WARNING)
            
            # This may not be an error if pydbvolve is being used in distributed development
            # Give the benefit of the doubt here.
            return 0
    else:
        targetIx = find_migration_file_version(config, migrations, targetVersion)
    
    if targetIx is None:
        write_log(config, "ERROR:: Could not find target migration version '{}'".format(config['version']), level=logging.ERROR)
        return 31
    
    if config.get('sequential', True) and currentVersion:
        startIx = find_migration_file_version(config, migrations, currentVersion['version'])
        if startIx is None:
            startIx = find_migration_file_version(config, migrations, currentVersion['version'], prior=True)
            if startIx is not None:
                startIx += 1    # This will allow the decrement to put us where we want to be
        
        if startIx is None:
            startIx = targetIx
        elif startIx == targetIx: # Sanity check!
            msg = "INFO: Database schema is already at this version (current = {}; target = {})".format(currentVersion['version'], targetVersion)
            if config.get('chatty'):
                print(msg)
            write_log(config, msg)
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
        return 32
    
    if baselineVersion and (migrations[targetIx]['sort_version'] < baselineVersion['sort_version']):
        write_log(config, "ERROR:: The target version is behind the baseline version (target = {}; baseline = {})".format(config['version'], baselineVersion['version']), level=logging.ERROR)
        return 33
    
    if baselineVersion and (migrations[startIx]['sort_version'] < baselineVersion['sort_version']):
        write_log(config, "ERROR:: The starting version is behind the baseline version (current = {}; baseline = {})".format(migrations[startIx]['version'], baselineVersion['version']), level=logging.ERROR)
        return 34
    
    try:
        rc = run_migration_job(config, migrations, startIx, targetIx, -1)
    except Exception as e:
        write_log(config, "EXCEPTION {}:: running migration job: {}".format(type(e).__name__, e), level=logging.ERROR)
        return 35
    else:
        if not rc:
            return 36

    currentVersion = get_current(config)
    if currentVersion:
        msg = "Current database version is: {}".format(currentVersion['version'])
        if config.get('chatty'):
            print(msg)
        write_log(config, msg)

    return 0
# End run_downgrade


def create_migration_id(config):
	return uuid.uuid4()
# End create_migration_id


def create_migration(config):
	migration_id = create_migration_id(config)
	
# End create_migration

