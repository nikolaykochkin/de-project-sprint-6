insert into l_user_message_from(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
select hash(hu.hk_user_id, hd.hk_message_id) as hk_l_groups_dialogs,
       hu.hk_user_id                         as hk_user_id,
       hd.hk_message_id                      as hk_message_id,
       now()                                 as load_dt,
       's3'                                  as load_src
from dialogs as d
         left join h_users as hu on d.message_from = hu.user_id
         left join h_dialogs as hd on d.message_id = hd.message_id
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from l_user_message_from);