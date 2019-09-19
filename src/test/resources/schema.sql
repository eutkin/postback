create table postback
(
    id      int identity primary key,
    user_id varchar(100),
    aim     smallint,
    code    varchar(100),
    created timestamp
);

create table mapping
(
    source  varchar(100) not null primary key,
    aim     varchar(30)  not null,
    user_id varchar(30)  not null,
    code    varchar(30)  not null
);
