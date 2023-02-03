insert into s_auth_history (hk_group_log_id, user_id_from, event, event_dt, load_dt, load_src)
select hgl.hk_group_log_id as hk_group_log_id,
       gl.user_id_from     as user_id_from,
       gl.event            as event,
       gl.datetime         as event_dt,
       now()               as load_dt,
       's3'                as load_src
from h_group_logs hgl
         left join group_log gl on hgl.group_log_id = gl.id;
