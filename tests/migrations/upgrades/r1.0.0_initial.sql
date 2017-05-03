-- Create some base tables
-- person
create table person 
(
    id int primary key,
    surname text not null,
    forename text not null,
    midname text,
    email_address text not null,
    phone_mobile text,
    school_id int
);
--RUN

create unique index ix01_person on person(email_address);
--run

create index ix02_person on person(surname, forename);
--run

create index ix03_person on person(school_id);
--run

-- school
create table school
(
    id int primary key,
    name text not null,
    short_name text not null
);
--run

insert into school (id, name, short_name) values (1, 'Teamworks Corporate', 'TW-CORP');
--run

