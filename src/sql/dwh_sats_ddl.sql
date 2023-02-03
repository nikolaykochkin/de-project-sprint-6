drop table if exists KK91YANDEXRU__DWH.s_admins;

create table if not exists KK91YANDEXRU__DWH.s_admins
(
    hk_admin_id int not null
        constraint fk_s_admins_l_admins
            references KK91YANDEXRU__DWH.l_admins (hk_l_admin_id),
    is_admin    boolean,
    admin_from  datetime,
    load_dt     datetime,
    load_src    varchar(20)
)
    order by load_dt
    segmented by hk_admin_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.s_user_chatinfo;

create table if not exists KK91YANDEXRU__DWH.s_user_chatinfo
(
    hk_user_id int not null
        constraint fk_s_user_chatinfo_h_users
            references KK91YANDEXRU__DWH.h_users (hk_user_id),
    chat_name  varchar(200),
    load_dt    timestamp,
    load_src   varchar(20)
)
    order by load_dt
    segmented by hk_user_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists KK91YANDEXRU__DWH.s_group_private_status;

create table if not exists KK91YANDEXRU__DWH.s_group_private_status
(
    hk_group_id int not null
        constraint fk_s_group_private_status_h_groups
            references KK91YANDEXRU__DWH.h_groups (hk_group_id),
    is_private  boolean,
    load_dt     timestamp,
    load_src    varchar(20)
)
    order by load_dt
    segmented by hk_group_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.s_group_name;

create table if not exists KK91YANDEXRU__DWH.s_group_name
(
    hk_group_id int not null
        constraint fk_s_group_name_h_groups
            references KK91YANDEXRU__DWH.h_groups (hk_group_id),
    group_name  varchar(100),
    load_dt     timestamp,
    load_src    varchar(20)
)
    order by load_dt
    segmented by hk_group_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.s_dialog_info;

create table if not exists KK91YANDEXRU__DWH.s_dialog_info
(
    hk_message_id int not null
        constraint fk_s_dialog_info_h_dialogs
            references KK91YANDEXRU__DWH.h_dialogs (hk_message_id),
    message       varchar(1000),
    message_from  int,
    message_to    int,
    load_dt       timestamp,
    load_src      varchar(20)
)
    order by load_dt
    segmented by hk_message_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.s_user_socdem;

create table if not exists KK91YANDEXRU__DWH.s_user_socdem
(
    hk_user_id int not null
        constraint fk_s_user_socdem_h_users
            references KK91YANDEXRU__DWH.h_users (hk_user_id),
    country    varchar(100),
    age        int,
    load_dt    timestamp,
    load_src   varchar(20)
)
    order by load_dt
    segmented by hk_user_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists KK91YANDEXRU__DWH.s_auth_history;

create table if not exists KK91YANDEXRU__DWH.s_auth_history
(
    hk_group_log_id int not null
        constraint fk_s_auth_history_l_user_group_activity
            references KK91YANDEXRU__DWH.h_group_logs (hk_group_log_id),
    user_id_from             int,
    event                    varchar(10),
    event_dt                 timestamp,
    load_dt                  timestamp,
    load_src                 varchar(20)
)
    order by load_dt
    segmented by hk_group_log_id all nodes
    partition by load_dt::date
        group by calendar_hierarchy_day(load_dt::date, 3, 2);
