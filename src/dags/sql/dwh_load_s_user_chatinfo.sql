insert into s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
select hu.hk_user_id as hk_user_id,
       u.chat_name   as chat_name,
       now()         as load_dt,
       's3'          as load_src
from h_users as hu
         left join users u on hu.user_id = u.id;