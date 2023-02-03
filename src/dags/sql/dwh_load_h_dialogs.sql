insert into h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
select hash(message_id) as hk_message_id,
       message_id       as message_id,
       message_ts       as message_ts,
       now()            as load_dt,
       's3'             as load_src
from dialogs
where hash(message_id) not in (select hk_message_id from h_dialogs);