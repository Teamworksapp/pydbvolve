-- add user_login table
create table user_login
(
    id int primary key,
    user_id int not null,
    login_ts timestamp not null,
    is_successful int not null
);
--run

create index ix01_user_login on user_login(user_id);
--run

create index ix02_user_login on user_login(login_ts);
--run

