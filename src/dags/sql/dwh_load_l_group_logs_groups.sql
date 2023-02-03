insert into l_group_logs_groups(hk_l_group_logs_groups, hk_group_log_id, hk_group_id, load_dt, load_src)
select hash(hgl.hk_group_log_id, hg.hk_group_id) as hk_l_group_logs_groups,
       hgl.hk_group_log_id                       as hk_group_log_id,
       hg.hk_group_id                            as hk_group_id,
       now()                                     as load_dt,
       's3'                                      as load_src
from group_log as gl
         left join h_group_logs as hgl on gl.id = hgl.group_log_id
         left join h_groups as hg on gl.group_id = hg.group_id
where hash(hgl.hk_group_log_id, hg.hk_group_id) not in (select hk_l_group_logs_groups from l_group_logs_groups);