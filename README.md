[![Build Status](https://travis-ci.org/Teamworksapp/pydbvolve.svg?branch=master)](https://travis-ci.org/Teamworksapp/pydbvolve)

# pydbvolve

pydbvolve is a stand-alone database migration tool written in **[Python 3](https://www.python.org/)** and incorporating these features:

* Ability to run SQL scripts
* Ability to run complex transformations written as Python 3 scripts
* Ability to modify aspects of the program via the config file: itself a Python 3 script
* Ability to run pre/post actions at the exection, migration file, and statement level
* Ability to apply versioned scripts sequentially
* Supports maintaining upgrade and downgrade operations
* Ability to mark a baseline version
* Ability to produce a report of all migrations applied to a schema
* Ability to test if the database is at a requested version
* Ability to produce information about the current database version
* Works with **any database** for which there is a **Python 3** module

---

## Purpose

### Problem

Although many web frameworks have a migration tool built into them, there are times where the database transformations are not executed correctly. This often causes the developer to have to navigate a nest of classes and functions not normally used to stay within the paradigm of the ORM or other class structure that is the interface to the database. 

There are other stand-alone migration tools available, but many of them are written in Java and would mean incorporating another language stack into either the web project or into the infrastructure stack. This may be undesirable given so many new web application are written using scripted languages (Python, Ruby) or Golang.

### Solution

Develop a database migration tool written in Python! Make this solution require the bare minimum of dependencies and make it easily deployable. Enter **pydbvolve**.

The only base dependency are that it runs via Python 3. The only database dependencies are those that must be installed to support the Python interface to the database engine.

---

## Installation

pydbvolve can be installed from this repo or can be installed from [PyPI](https://pypi.python.org/pypi/pydbvolve)

It is up to the user to ensure that the appropriate database module(s) are installed for communication with the engine plus any other dependencies that are needed for the specific project or infrastructure.

## Invocation

pydbvolve is a command-line tool that is meant to be integrated into development or DevOps as any other script would. It generally is quiet, preferring to use return codes over exceptions, but will use exceptions in extreme cases. It will write logs using Python logging module.

### Syntax

```
pydbvolve [-h | --help] --config CONFIG_FILE [--force] [--version] [--libversion]
          (--baseline B_VERSION | --upgrade U_VERSION | --upgrade-latest |
           --downgrade D_VERSION | --info | --migration-log | --verify V_VERSION)
```

#### Required Arguments

**--config CONFIG_FILE**  
Specify the configuration file to use.

Plus **one** of:

**--baseline B_VERSION**  
Set a particular version tag as the **baseline**  
**--baseline-current**  
Set the current version tag as the **baseline**. A migration must be run before using this option.  
**--upgrade U_VERSION**  
Upgrade database from the current version to a newer version  
**--upgrade-latest**  
Upgrade database from the current version to the latest known version  
**--downgrade D_VERSION**  
Downgrade database from the current version to an earlier version  
**--downgrade-baseline**  
Downgrade database from the current version to the baseline version. A baseline must have been previously set.  
**--verify V_VERSION**  
Check to see if the current version matches the specified version. Returns a zero return code on match.  
**--info**  
Write known information about the current migration to stdout  
**--migration-log**  
Write a plain-text report of all migrations to stdout

#### Optional Arguments

**--force**  
Apply an out-of-order migration.  
**--verbose**  
Copy logs to stdout  
**--version**  
Write CLI version to stdout  
**--libversion**  
Write module version to stdout

## Configration

pydbvolve requires a configuration file. This configuration file adapts the program for your environment. This configuration file differs from others in that it is a Python 3 script as well. By writing the necessary functions, you can change defaults, protect database credentials and connect to whatever database you choose.

To alter the default configuration, simply write a Python 3 file of function definitions with the same name having the same arguments and those functions will override the defaults from pydbvolve. You only have to override the functions that provide the values you wish to change. 

The config file does not require a .py extension.

The configuration logic was designed this way to enable the config file to be an interface into some credential encrpytion scheme for protection and to allow for advanced action to be taken if need at config time; to set the pre/post actions; and to be able to interact with various database module initializations.

The structure use to hole the configuration values is an instance of **dict**. This _must not_ change.

### Configuration Functions

#### Initial Configuration Functions

| Function               | Return Type | Definition 
| ---------------------- | ----------- | -----------
| get_base_dir(config_file_path)         | str         | Returns the base directory for the migrations. Default is **<config_file_dir>/pydbvolve** Config key is **base_dir**.
| get_migration_base_dir(base_dir) | str       | Returns the base directory for the migration subdirectories. Default is base_dir, 'migrations'). Config key is **migration_dir**.
| get_migration_upgrade_dir(migration_base_dir) | str    | Returns the directory that will contain the upgrade scripts. Default is migration_base_dir, 'upgrades'). Config key is **migration_upgrade_dir**.
| get_migration_downgrade_dir(migration_base_dir) | str  | Returns the directory that will contain the downgrade scriptes. Default is migration_base_dir, 'downgrades'). Config key is **migration_downgrade_dir**.
| get_log_dir(base_dir)          | str         | Returns the directory that will contain the log files. Default is base_dir, "logs") Config key is **log_dir**.
| get_migration_table_name() | str     | Returns the migration table name. Default is **__migrations__**. Config key is **migration_table_name**
| get_migration_table_schema() | str   | Returns the name of the schema in which the migration table should reside. Default is **public**. Config key is **migration_table_schema**.
| get_positional_variable_marker() | str | Returns the string that should be used to indicate a positional variable for the database module used. This is used internally for creating the migration records to be stored in the migration table. Default is **%s**. Config key is **positional_variable_marker**
| get_file_name_regex() | SRE_Pattern instance | Returns the regex that will parse the migration file names into the component information used for versioniing and file type determination. Default is re.compile('^([^\_]+)\_([^.]+).(sql\|py)$'). Config key is **filename_regex**. Config key is **filename_regex**
| get_sql_statement_sep() | SRE_Pattern instance | Returns the regex that will separate individual SQL statements in a sql file. This is only used at runtime and not stored in the config, but it can be overridden. Default is re.compile('^\\s*--\\s*run\\s*$', flags=re.MULTILINE\|re.IGNORECASE)

#### Post-Initial Configuration Functions

This is a list of configuration function that rely on the completed initial configuration. These functions expect 1 parameter which will be the config dict.

| Function               | Return Type | Definition 
| ---------------------- | ----------- | -----------
| get_migration_user(config) | str     | Returns the user that invoked the process. Used for logging. Default uses getpass.getuser()
| set_log_file_name(config) | dict     | Sets a log file name in **config['log_file_name']**. Returns config. Override this for an empty function body or set config['log_file_name'] = None if not using file-based logging.
| set_logger_name(config) | dict       | Sets a name for the logger in **config['logger_name']**. Returns config.
| set_logger_level(config) | dict      | Sets the python logger level at **config['logger_level']**. The default is logger.INFO level. Returns config.
| setup_error_log_handler(config) | dict | Set a separate stream-based log handler to handle logger.WARNING and logger.ERROR messages. The logger must be created first and stored at **config['logger']**. Returns config. Override to return config or set config['log_file_name'] to None to cancel the handler setup.
| setup_file_logger(config) | dict     | Setup a python logger with a file-based log handler. The file name is taken from **config['log_file_name']** and the logger name is taken from **config['logger_name']** and the logger level is taken from **config['logger_level']**. A separate stream-based error handler will be set for warnings and errors by calling **setup_error_log_handler**. If **config['verbose']** is True, a separate stream-based handler will be attached to the logger to echo all messages. Returns config. Override to alter settings.
| setup_stream_logger(config) | dict   | Setup a python logger with a stream-based log handler. The logger name is taken from **config['logger_name']** and the logger level is taken from **config['logger_level']**. No other handlers will be set. Returns config. Override to alter settings.
| setup_log(config)         | dict     | Setup logging for run. Calls **set_logger_name**, **set_log_file_name** and **set_logger_level** to initialize the config. If **config['log_file_name']** has a value, **setup_file_logger** is called otherwise **setup_stream_logger** is called. Returns config.
| close_log(config)         | None     | Flushes all log handlers and closes the logger instance.

#### Database Connectivity Functions

| Function               | Return Type | Definition 
| ---------------------- | ----------- | -----------
| get_db_credentials(config) | dict    | Get the credentials needed to logon to the database and return them as a **dict** instance. These requirements may vary depending on the database module. Please refer to that documentation for the required values. The only value that pydbvolve wants is a database user for logging. Store this database username value in the credentials dict with a key named **user**.
| get_db_user(config, credentials) | str | Returns the database username. Default is credentials.get('user', 'unknown'). This is used for logging.
| get_db_connection(config, credentials) | database connection class instance | Uses the values in the credentials dict to create a connection to the database.

#### Trigger Functions

| Function               | Return Type | Definition 
| ---------------------- | ----------- | -----------
| pre_execution()        | None        | Execute arbitrary Python 3 statements before the pydbvolve execution begins. On error, raise a **MigrationError** exception.
| post_execution()       | None        | Execute arbitrary Python 3 statements before pydbvolve exits. On error, raise a **MigrationError** exception.
| pre_script(config, migration) | None | Execute arbitrary Python 3 statements before a migration script is executed. Has two positional arguments to provide access to the config and to the migration script information (also a dict instance). On error, raise a **MigrationError** exception.
| post_script(config, migration) | None | Execute arbitrary Python 3 statements after a migration script is executed. Has two positional arguments to provide access to the config and to the migration script information (also a dict instance). On error, raise a **MigrationError** exception.
| pre_statement(config, migration, statement) | None | Only fires for SQL migrations. Execute arbitrary Python 3 statements before a SQL statement is executed. Has three positional arguments to provide access to the config and to the migration script information (also a dict instance) and the third parameter is the statement string. On error, raise a **MigrationError** exception.
| post_statement(config, migration, statement) | None | Only fires for SQL migrations. Execute arbitrary Python 3 statements after a SQL statement is executed. Has three positional arguments to provide access to the config and to the migration script information (also a dict instance) and the third parameter is the statement string. On error, raise a **MigrationError** exception.

---

### Database Operations

pydbvolve makes heavy use of context managers and **expects** that any database connection and cursor class support being used as a context manager. If your database module does not support context managers, please write subclasses as necessary to support the instances being used like 

```Python
with dbmodule.connect(**params) as conn:
    with conn.cursor() as cur:
        cur.execute(sqlstuff)
```

#### SQLite Considerations

See the snippets/sqlite.py file for subclasses and functions that should be used when your target database to migrate is a sqlite3 database.

---

### Migration Table

pydbvolve doesn't care about what happens in the migration files. Even if they contained selects, the results would not be fetched. However, for pydbvolve to properly interact with the database for migration table operations, all cursors **must** return rows that are real Python 3 dict instances. If they do not return dict instances, proper operation cannot be ensured.

The migration table serves three purposes:

1. Denote the current migration version
2. Denote the baseline migration version (if set)
3. Provide a log of all migrations to the database schema.

---

## Migrations

### Transactions

pydbovlve will run migrations on a script-by-script basis in an all-or-nothing fashion. This means that commits will be made after each script run, but no commit will be implicitly be run during a script execution. If commits are run during a script execution it is the responsibility of the script author that those are necessary and are handled properly in any kind of rollback operation.

It is recommended that commits, nor any other type of explicit transaction boundary not be done in SQL scripts in case of error or abort because the database could be in a partially-migrated state in either case. Better to let pydbvolve handle the transaction boundary so if any one script fails, all changes that script made will be rolled back.

If your database connection class has an autocommit setting, it should be set to False.

When a migration script completes, the migration table will be updated with that script's information and a commit will be executed for all of the changes. This will allow for selective downgrades if, for example, three upgrade scripts were applied, only the first two succeeded and the downgrade operation only means to undo the second script's changes.

### Scripts

Migration scripts can be SQL files or python files if the transforms are sufficiently complex. Please ensure that **any** script file has an empty line at the end. It will ensure proper parsing or compilation.

All upgrade scripts are to be put in the designated upgrade migrations directory and all downgrade scripts are to be put in the designated downgrade migrations directory. The scripts should be named like:

```
VERSION_DESCRIPTION.sql
```

or 

```
VERSION_DESCRIPTION.py
```

Where VERSION can be a string like:

```
textMAJOR.MINOR.PATCH
```

or

```
MAJOR.MINOR.PATCH
```

or 

```
MAJOR
```

Where MAJOR, MINOR, and PATCH are integer numbers with optional zero or space left-padding. 

Migration scripts used by pydbvovle should **always** be named for the target version of the database that will be the result of that script operation.

For example:  
If the database is currently at 3.10.0 and an upgrade is required to 3.11.0, the upgrade action script should be named similarly to

```
r3.11.0_db_upgrade.sql
```

This script should contain all of the schema and data transforms needed to set the database for this version.

The script that will undo these actions should be named like

```
r3.10.0_downgrade_from 3_11.sql
```

Because the target version from the left side of the string is the resulting database version of the 3.11 undo.

#### Script Execution

pydbvolve will gather all migration files from the upgrade or downgrade directory (based on the operation) matching files using the filename_regex from the config and will get the sortable version from the file name. The sortable version is a tuple of integers (not strings of digits) and sort the files according to that. This is done so that version 1.11 follows 1.10 instead of 1.1.

Based on the migration action, sequential migrations are applied from the current known migration to the specified earlier or later migration. 

A downgrade cannot be performed if there is no current migration. If a baseline has been set, pydbvolve will prevent downgrades to earlier than the baseline.

Script files for the current version as well as the target version **must** exist in the migration action directory. Only baseline versions are allowed to not be actual script files since a baseline merely serves as an origin marker.

### Forcing a migration

By default pydbvolve will attempt to apply scripts in sort-version order from current to target. However, there is the ability to force in a script that is out-of-order. Out-of-order application will only execute the target script based on its version string match and will not attempt to execute any other. Since this may break ordering based on the other scripts in migration action directories, it is up to the user to either manage the migration scripts or manually execute pydbvolve until it is in a state to run sequentially once more.

### SQL Migrations

SQL migrations are merely files of SQL statements for the target database engine. When running a SQL script, pydbvolve will attempt to read and parse the file into individual statements to run. Since SQL is a complex language with extensions and syntax that differs from engine to engine, it is beyond the scope of pydbvolve to be a SQL token parser. Instead it relies on the user to add SQL statement separators immediately after statements to run. By default, this separator is the text **-- run** and must be on its own line.

Ex.

```sql
alter table foo add column quatloos integer not null default -1;
-- run

update foo
   set quatloos = kelvins * splunks - feens;
-- run
```

This will allow the parser to discreetly get the alter and update statements and execute them separately. Running a single statement is a requirement for some Python database modules.

### Python Migrations

When the transformations are sufficiently complex or rely on some external input or application, a Python script may be necessary. Python migration scripts should be coded for execution via Python 3.

A Python script file is just a python file of functions required to perform the transformation. pydbvolve will load these Python 3 scripts as modules. Python migration scripts should contain a function with the following definition as the entry point:

```Python
def run_migration(config, migration):
    # Your migration here!
    
    # Return a boolean value: True for successful migration, False for a failed migration. Or throw your own exception.
    return True if success else False
```

Config is the configuration dict. Along with all of the configuration data that was set at initialization, he database connection will be exposed via the key **conn** and the log writer function will be exposed via the **write_log** key. The migration dict instance will have the following keys:

**version**  
string version from the first part of the filename  
**description**  
The part of the filename between the version and the file type extension  
**filetype**  
The file type extension  
**filename**  
Unparsed file name  
**sort_version**  
Tuple used for sorting

The log writer is defined as follows:

```python
write_log(config, message)
```

If force_visibility is set to True, then the message is copied to stdout. Pass in the config variable for the config parameter and message is the string you wish to write to the log.

The body of the **run_migration** function will now execute self-contained in the module with only config and migration as the links from pydbvolve.

---

## Best Practices

* Make sure that autocommit on the connection class instance is set to False.
* All upgrade migrations should have downgrade migrations.
* Set the VERSION part of migration file names for the version the database will be after the script has run.
* Make sure that the SQL statement separator follows each discrete statement in your SQL migration files.
* Set a baseline before you start

```bash
pydbvolve --config my_config.conf --baseline r1.0.0
```

* Always check your return codes!

```bash
pydbvolve --config my_config.conf --upgrade r10.20.30 && run-version-tests || send-panic-email
```

* Use verfiy in your startup process

```bash
pydbvolve --config my_config.conf --verify ${REQUIRED_DB_VERSION} && run-app-startup-script
```

### A Simple Recipe

A simple recipe is to simply always use the **--upgrade-latest** switch. When this switch is used, the latest migration file version is checked against the latest version in the migration table in the db. If they match, then pydbvolve will exit with success (0).

```bash
pydbvolve --config my_config.conf --upgrade-latest || do-panic-stuff
```

---

## Sample Config File

This config file was designed to operate with sqlite3 databases. The sqlite3 database does not use schemata. So the get_migration_table_schema() call just returns an empty string.

```python
import sqlite3
import getpass
import os
import re


# Could have used sqlite3.Row, but simpler is better, in this case. The main script works with dict types.
def dict_factory(cur, row):
    return {col[0]: row[ix] for ix, col in enumerate(cur.description)}
# End dict_factory


# This is a subclass of sqlite3.Cursor that includes rudimentary __enter__, __exit__ 
# methods so it can be used in with context manager statements
class CMCursor(sqlite3.Cursor):
    def __enter__(self):
        return self
    
    def __exit__(self, e_type, e_value, e_tb):
        self.close()
# End class CMCursor


# This is a subclass of the sqlite3.Connection class. It does nothing but force
# dict_factory as the row factory and the CMCursor as the cursor factory
class CMConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.row_factory = dict_factory
    
    def cursor(self, *args, **kwargs):
        return super().cursor(factory=CMCursor)
# End class CMConnection


# For testing purposes, set the base dir to the working dir
def get_base_dir():
    return './tests'
# End get_base_dir

# sqlite module uses a question mark character to denote a positional variable
# in sql statements. Set this so the migration table can be added to and updated 
# successfully
def get_positional_variable_marker():
    return '?'
# End get_positional_variable_marker


# sqlite3 has no concept of a schema, so we return empty string here.
def get_migration_table_schema():
    return ''
# End get_migration_table_schema


# sqlite3 has no concept of credentials, so we set some useful stuff here.
def get_db_credentials(config):
    return {'user': getpass.getuser(), 'file': os.path.join(get_base_dir(), 'test_db.sqlite')}
# End get_db_credentials


# And here we make the connection and return the connection instance
def get_db_connection(config, credentials):
    conn = sqlite3.connect(credentials['file'], factory=CMConnection)
    return conn
# End get_db_connection
```

This config file was written to work with PostgreSQL with the migration table in the default public schema.

```python
import os
import psycopg2
import psycopg2.extras as pextr


def get_base_dir():
    # Assumes this will be executed from top-level dir
    base_path = os.path.join(os.path.abspath('.'), 'pydbvolve')
    return base_path
# End get_base_dir


def get_db_credentials(config):
    return {'user': 'postgres', 'password': None, 'database': 'postgres', 'host': 'localhost'}
# End get_db_credentials


def get_db_connection(config, credentials):
    conn = psycopg2.connect(host=credentials['host'],
                            database=credentials['database'],
                            user=credentials['user'],
                            password=credentials.get('password'),
                            port=credentials.get('port', 5432),
                            cursor_factory=pextr.RealDictCursor)
    conn.set_session(autocommit=False)
    
    return conn
# End get_db_connection
```

## Sample Python migration

This is a sample migration file designed to run against a sqlite3 database (note the **?** positional argument token).

```python
import bcrypt


def create_user_table(conn):
    create_sql = """
create table users
(
    id int primary key,
    username text not null,
    hashpw text not null,
    salt text not null,
    last_login_ts timestamp
);
"""
    index_sql = """
create index ix01_users on users(username);
"""
    
    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(index_sql)
# End create_user_table


def add_default_user(conn):
    username = 'defuser'
    salt = bcrypt.gensalt()
    hashpw = bcrypt.hashpw('defpw', salt)
    sql = """
insert into users (id, username, hashpw, salt) values (?, ?, ?, ?);
"""
    values = (1, username, hashpw, salt)
    with conn.cursor() as cur:
        cur.execute(sql, values)
# End add_default_user


def run_migration(config, migration):
    conn = config['conn']
    config['write_log'](config, "Creating user table")
    create_user_table(conn)
    config['write_log'](config, "Adding default user")
    add_default_user(conn)

    return True
# End run_migration
```

---

# Advanced

## Embedding

pydbvolve is distributed as a command-line tool, but that tool is just a CLI to the pydbvolve module. This module can be imported into any Python 3 project that needs database migration functionality.

The entry point into the module is the **run_migration** function.

```Python
run_migration(configFileName, action, version, sequential, verbose=False)
```

This function handles the initialization and execution of the migration action specified. The parameters are:  
**configFileName**  
A str instance that is the full path and file name of the config file.  
**action**  
A str instance that is the migration action. Valid values are:

| action                 | Corresponding CLI Argument
| ---------------------- | -------------------------------
| 'upgrade'              | --upgrade, --upgrade-latest 
| 'downgrade'            | --downgrade, --downgrade-baseline
| 'baseline'             | --baseline, --baseline-current
| 'info'                 | --info, --baseline-info
| 'verify'               | --verify
| 'log'                  | --migration-log

The value of action is always tested against **pydbvolve.VALID_ACTIONS**  
**version**  
A str instance that has the version string to act on. For the **log** and **info** actions, this can be an empty string. To upgrade to the latest version, this should be set to **pydbvolve.LATEST_VERSION**. To downgrade to the baseline version, set version to **pydbvolve.BASELINE_VERSION**. To baseline the current version, set the version to **pydbvolve.CURRENT_VERSION**. The **info** command will return the current version information if **pydbvolve.LATEST_VERSION** is passed as the version. Otherwise, info will try to return the latest applied information for a specific version tag outside of the special values.  
**sequential**  
Bool instance. If you wish to apply migrations sequentially, set this to **True**. To apply an out-of-order migration, set it to **False**  
**verbose**  
Bool instance. If **True** then logs are also written to stdout.

The version of the module can be checked against the tuple **pydbvolve.\_\_VERSION\_\_** or the str **pydbvolve.\_\_VERSION_STRING\_\_**

---

## Configuration

The config file is itself a Python 3 file of functions. This file will be read, compiled, and executed in the memory-space of the base process. This is the mechanism for overriding the default behavior of the program. If you import pydbovlve like

```python
import pydbvolve
```

the modifications will only affect objects under the **pydbvolve** namespace. However, if you import pydbvolve like

```python
from pydbovlve import *
```

the modifications will affect the main process.

Be aware of this as your write additional functions for the config file or if your main functions have the same names as those within the pydbvolve module. 

This is the difference between the config file and Python migration files. Python migration files are **always** loaded as if by an **import** statement as in 

```python
import your_python_migration
```

and can never _implicitly_ interfere with the main process or the pydbvolve module.
