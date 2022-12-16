create table counters (
    persistence_id varchar(100) primary key,
    sequence_number bigint not null ,
    timestamp bigint not null ,
    counter int not null
);