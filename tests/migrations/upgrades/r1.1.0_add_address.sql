-- add address tables
create table address_type
(
    id int primary key,
    label text not null
);
--run

create table address
(
    id int primary key,
    address_type_id int not null,
    address_name text,
    address_subname text,
    street_1 text not null,
    street_2 text,
    city text not null,
    state text not null,
    zipcode text not null
);
--run

create table address_link
(
    id int primary key,
    link_type text not null,
    link_id int not null,
    address_id int not null
);
--run

create index ix01_address_link on address_link (link_type, link_id);
--run

create index ix02_address_link on address_link (address_id);
--run

-- set some types
insert into address_type (id, label) values (1, 'Home');
--run

insert into address_type (id, label) values (2, 'Work');
--run

insert into address_type (id, label) values (3, 'School');
--run

insert into address_type (id, label) values (4, 'Summer');
--run

insert into address_type (id, label) values (5, 'Winter');
--run

