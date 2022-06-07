drop table if exists users_tasks;
drop table if exists users;
drop table if exists tasks;
drop table if exists projects;
drop table if exists clients;
drop table if exists complex_items;



create table clients (
    id integer,
    name text NOT NULL,
    primary key (id)
) ENGINE = MergeTree();

create table projects (
    id integer,
    name text NOT NULL,
    client_id integer, -- references clients(id),
    primary key (id)
) ENGINE = MergeTree();

-- create view projects_view as select * from projects;

create table tasks (
    id integer,
    name text NOT NULL,
    project_id integer, -- references projects(id),
    primary key (id)
) ENGINE = MergeTree();

create table users (
    id integer,
    name text NOT NULL,
    primary key (id)
) ENGINE = MergeTree();

create table users_tasks (
  user_id integer NOT NULL, -- references users(id),
  task_id integer NOT NULL, -- references tasks(id),
  primary key (task_id, user_id)
) ENGINE = MergeTree();

create table complex_items (
    id integer NOT NULL,
    name text,
    settings text,
    primary key (id)
) ENGINE = MergeTree();

create table trips (
    `trip_id` UInt32,
    `vendor_id` Enum8('1' = 1, '2' = 2, '3' = 3, '4' = 4, 'CMT' = 5, 'VTS' = 6, 'DDS' = 7, 'B02512' = 10, 'B02598' = 11, 'B02617' = 12, 'B02682' = 13, 'B02764' = 14, '' = 15),
    `pickup_date` Date,
    `pickup_datetime` DateTime,
    `dropoff_date` Date,
    `dropoff_datetime` DateTime,
    `store_and_fwd_flag` UInt8,
    `rate_code_id` UInt8,
    `pickup_longitude` Float64,
    `pickup_latitude` Float64,
    `dropoff_longitude` Float64,
    `dropoff_latitude` Float64,
    `passenger_count` UInt8,
    `trip_distance` Float64,
    `fare_amount` Float32,
    `extra` Float32,
    `mta_tax` Float32,
    `tip_amount` Float32,
    `tolls_amount` Float32,
    `ehail_fee` Float32,
    `improvement_surcharge` Float32,
    `total_amount` Float32,
    `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4),
    `trip_type` UInt8,
    `pickup` FixedString(25),
    `dropoff` FixedString(25),
    `cab_type` Enum8('yellow' = 1, 'green' = 2, 'uber' = 3),
    `pickup_nyct2010_gid` Int8,
    `pickup_ctlabel` Float32,
    `pickup_borocode` Int8,
    `pickup_ct2010` String,
    `pickup_boroct2010` String,
    `pickup_cdeligibil` String,
    `pickup_ntacode` FixedString(4),
    `pickup_ntaname` String,
    `pickup_puma` UInt16,
    `dropoff_nyct2010_gid` UInt8,
    `dropoff_ctlabel` Float32,
    `dropoff_borocode` UInt8,
    `dropoff_ct2010` String,
    `dropoff_boroct2010` String,
    `dropoff_cdeligibil` String,
    `dropoff_ntacode` FixedString(4),
    `dropoff_ntaname` String,
    `dropoff_puma` UInt16
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY pickup_datetime;


insert into clients values (1, 'Microsoft');
insert into clients values (2, 'Apple');

insert into users values (1, 'Angela Martin');
insert into users values (2, 'Michael Scott');
insert into users values (3, 'Dwight Schrute');

insert into projects values (1, 'Windows 7', 1);
insert into projects values (2, 'Windows 10', 1);
insert into projects values (3, 'IOS', 2);
insert into projects values (4, 'OSX', 2);
insert into projects values (5, 'Orphan', NULL);

insert into tasks values (1, 'Design w7', 1);
insert into tasks values (2, 'Code w7', 1);
insert into tasks values (3, 'Design w10', 2);
insert into tasks values (4, 'Code w10', 2);
insert into tasks values (5, 'Design IOS', 3);
insert into tasks values (6, 'Code IOS', 3);
insert into tasks values (7, 'Design OSX', 4);
insert into tasks values (8, 'Code OSX', 4);

insert into users_tasks values (1, 1);
insert into users_tasks values (1, 2);
insert into users_tasks values (1, 3);
insert into users_tasks values (1, 4);
insert into users_tasks values (2, 5);
insert into users_tasks values (2, 6);
insert into users_tasks values (2, 7);
insert into users_tasks values (3, 1);
insert into users_tasks values (3, 5);


insert into complex_items values (1, 'One', '{"foo":{"int":1,"bar":"baz"}}');
insert into complex_items values (2, 'Two', '{"foo":{"int":1,"bar":"baz"}}');
insert into complex_items values (3, 'Three', '{"foo":{"int":1,"bar":"baz"}}');

INSERT INTO trips
    SELECT * FROM s3(
        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_1.gz',
        'TabSeparatedWithNames'
    );

CREATE DICTIONARY taxi_zone_dictionary (
    LocationID UInt16 DEFAULT 0,
    Borough String,
    Zone String,
    service_zone String
)
PRIMARY KEY LocationID
SOURCE(HTTP(
    url 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv'
    format 'CSVWithNames'
))
LIFETIME(0)
LAYOUT(HASHED());