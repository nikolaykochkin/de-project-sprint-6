insert into s_group_private_status(hk_group_id, is_private, load_dt, load_src)
select hg.hk_group_id as hk_group_id,
       g.is_private   as is_private,
       now()          as load_dt,
       's3'           as load_src
from h_groups as hg
         left join groups g on hg.group_id = g.id;