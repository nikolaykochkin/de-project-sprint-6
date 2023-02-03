insert into l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
select hash(hd.hk_message_id, hg.hk_group_id) as hk_l_groups_dialogs,
       hd.hk_message_id                       as hk_message_id,
       hg.hk_group_id                         as hk_group_id,
       now()                                  as load_dt,
       's3'                                   as load_src
from dialogs as d
         left join h_dialogs as hd on d.message_id = hd.message_id
         left join h_groups as hg on d.message_group = hg.group_id
where d.message_group is not null
  and hash(hd.hk_message_id, hg.hk_group_id) not in (select hk_l_groups_dialogs from l_groups_dialogs);