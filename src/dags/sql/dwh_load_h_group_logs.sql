insert into h_group_logs (hk_group_log_id, group_log_id, datetime, load_dt, load_src)
select hash(id) as hk_group_log_id,
       id       as group_log_id,
       datetime as datetime,
       now()    as load_dt,
       's3'     as load_src
from group_log
where hash(id) not in (select hk_group_log_id from h_group_logs);