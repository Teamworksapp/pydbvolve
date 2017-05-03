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
    username = 'teamworks'
    salt = bcrypt.gensalt()
    hashpw = bcrypt.hashpw(salt, salt)
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


