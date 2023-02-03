drop table if exists KK91YANDEXRU__DWH.l_user_message_from;

create table if not exists KK91YANDEXRU__DWH.l_user_message_from
(
    hk_l_user_message bigint primary key,
    hk_user_id        bigint not null
        constraint fk_l_user_message_from_user
            references KK91YANDEXRU__DWH.h_users (hk_user_id),
    hk_message_id     bigint not null
        constraint fk_l_user_message_from_message
            references KK91YANDEXRU__DWH.h_dialogs (hk_message_id),
    load_dt           datetime,
    load_src          varchar(20)
)
    order by load_dt
    segmented by hk_l_user_message all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.l_user_message_to;

create table if not exists KK91YANDEXRU__DWH.l_user_message_to
(
    hk_l_user_message bigint primary key,
    hk_user_id        bigint not null
        constraint fk_l_user_message_to_user
            references KK91YANDEXRU__DWH.h_users (hk_user_id),
    hk_message_id     bigint not null
        constraint fk_l_user_message_to_message
            references KK91YANDEXRU__DWH.h_dialogs (hk_message_id),
    load_dt           datetime,
    load_src          varchar(20)
)
    order by load_dt
    segmented by hk_l_user_message all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.l_admins;

create table if not exists KK91YANDEXRU__DWH.l_admins
(
    hk_l_admin_id int primary key,
    hk_user_id    int not null
        constraint fk_l_admins_user
            references KK91YANDEXRU__DWH.h_users (hk_user_id),
    hk_group_id   int not null
        constraint fk_l_admins_group
            references KK91YANDEXRU__DWH.h_groups (hk_group_id),
    load_dt       timestamp,
    load_src      varchar(20)
)
    order by load_dt
    segmented by hk_l_admin_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.l_groups_dialogs;

create table if not exists KK91YANDEXRU__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs int primary key,
    hk_message_id       int not null
        constraint fk_l_groups_dialogs_message
            references KK91YANDEXRU__DWH.h_dialogs (hk_message_id),
    hk_group_id         int not null
        constraint fk_l_groups_dialogs_group
            references KK91YANDEXRU__DWH.h_groups (hk_group_id),
    load_dt             timestamp,
    load_src            varchar(20)
)
    order by load_dt
    segmented by hk_l_groups_dialogs all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.l_group_logs_users;

create table if not exists KK91YANDEXRU__DWH.l_group_logs_users
(
    hk_l_group_logs_users int primary key,
    hk_group_log_id       int not null
        constraint fk_l_group_logs_users_group_log
            references KK91YANDEXRU__DWH.h_group_logs (hk_group_log_id),
    hk_user_id            int not null
        constraint fk_l_group_logs_users_user
            references KK91YANDEXRU__DWH.h_users (hk_user_id),
    load_dt               timestamp,
    load_src              varchar(20)
)
    order by load_dt
    segmented by hk_l_group_logs_users all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.l_group_logs_groups;

create table if not exists KK91YANDEXRU__DWH.l_group_logs_groups
(
    hk_l_group_logs_groups int primary key,
    hk_group_log_id        int not null
        constraint fk_l_group_logs_groups_group_log
            references KK91YANDEXRU__DWH.h_group_logs (hk_group_log_id),
    hk_group_id            int not null
        constraint fk_l_group_logs_groups_group
            references KK91YANDEXRU__DWH.h_groups (hk_group_id),
    load_dt                timestamp,
    load_src               varchar(20)
)
    order by load_dt
    segmented by hk_l_group_logs_groups all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.l_group_logs_users_from;

create table if not exists KK91YANDEXRU__DWH.l_group_logs_users_from
(
    hk_l_group_logs_users_from int primary key,
    hk_group_log_id            int not null
        constraint fk_l_group_logs_users_from_group_log
            references KK91YANDEXRU__DWH.h_group_logs (hk_group_log_id),
    hk_user_id                 int not null
        constraint fk_l_group_logs_users_from_user
            references KK91YANDEXRU__DWH.h_users (hk_user_id),
    load_dt                    timestamp,
    load_src                   varchar(20)
)
    order by load_dt
    segmented by hk_l_group_logs_users_from all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);