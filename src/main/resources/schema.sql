create table mapping
(
    source  varchar(100) not null primary key,
    aim     varchar(30)  not null,
    user_id varchar(30)  not null,
    code    varchar(30)  not null
);


create table postback
(
    id      bigserial primary key,
    source  varchar(100) not null references mapping (source),
    user_id varchar(100),
    aim     smallint,
    code    varchar(100),
    created timestamp    not null default current_timestamp
);

