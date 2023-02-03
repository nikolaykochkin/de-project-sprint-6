drop table if exists KK91YANDEXRU__DWH.h_users;

create table if not exists KK91YANDEXRU__DWH.h_users
(
    hk_user_id      int primary key,
    user_id         int,
    registration_dt timestamp,
    load_dt         timestamp,
    load_src        varchar(20)
)
    order by load_dt
    segmented by hk_user_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.h_groups;

create table if not exists KK91YANDEXRU__DWH.h_groups
(
    hk_group_id     int primary key,
    group_id        int,
    registration_dt timestamp,
    load_dt         timestamp,
    load_src        varchar(20)
)
    order by load_dt
    segmented by hk_group_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.h_dialogs;

create table if not exists KK91YANDEXRU__DWH.h_dialogs
(
    hk_message_id int primary key,
    message_id    int,
    message_ts    timestamp,
    load_dt       timestamp,
    load_src      varchar(20)
)
    order by load_dt
    segmented by hk_message_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.h_group_logs;

create table if not exists KK91YANDEXRU__DWH.h_group_logs
(
    hk_group_log_id int primary key,
    group_log_id    int,
    datetime        timestamp,
    load_dt         timestamp,
    load_src        varchar(20)
)
    order by load_dt
    segmented by hk_group_log_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

