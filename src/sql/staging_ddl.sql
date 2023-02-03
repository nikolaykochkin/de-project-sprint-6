drop table if exists KK91YANDEXRU__STAGING.dialogs;

create table if not exists KK91YANDEXRU__STAGING.dialogs
(
    message_id    int not null primary key,
    message_ts    timestamp,
    message_from  int,
    message_to    int,
    message       varchar(1000),
    message_group int
)
    order by message_id
    partition by message_ts::date
        group by calendar_hierarchy_day(message_ts::date, 3, 2);

drop table if exists KK91YANDEXRU__STAGING.users;

create table if not exists KK91YANDEXRU__STAGING.users
(
    id              int not null primary key,
    chat_name       varchar(200),
    registration_dt timestamp,
    country         varchar(200),
    age             int
)
    order by id;

drop table if exists KK91YANDEXRU__STAGING.groups;

create table if not exists KK91YANDEXRU__STAGING.groups
(
    id              int not null primary key,
    admin_id        int,
    group_name      varchar(100),
    registration_dt timestamp,
    is_private      boolean
)
    order by id, admin_id
    partition by registration_dt::date
        group by calendar_hierarchy_day(registration_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__STAGING.group_log;

create table if not exists KK91YANDEXRU__STAGING.group_log
(
    id identity primary key,
    group_id     int,
    user_id      int,
    user_id_from int,
    event        varchar(10),
    datetime     timestamp
)
    order by id, group_id, user_id
    partition by datetime::date
        group by calendar_hierarchy_day(datetime::date, 3, 2);

