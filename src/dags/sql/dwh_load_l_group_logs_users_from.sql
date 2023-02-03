insert into l_group_logs_users_from(hk_l_group_logs_users_from, hk_group_log_id, hk_user_id, load_dt, load_src)
select hash(hgl.hk_group_log_id, hu.hk_user_id) as hk_l_group_logs_users_from,
       hgl.hk_group_log_id                      as hk_group_log_id,
       hu.hk_user_id                            as hk_user_id,
       now()                                    as load_dt,
       's3'                                     as load_src
from group_log as gl
         left join h_group_logs as hgl on gl.id = hgl.group_log_id
         left join h_users as hu on gl.user_id_from = hu.user_id
where gl.user_id_from is not null
  and hash(hgl.hk_group_log_id, hu.hk_user_id) not in (select hk_l_group_logs_users_from from l_group_logs_users_from);