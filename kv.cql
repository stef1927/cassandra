create keyspace if not exists cvs_copy_ks with replication = {'class': 'SimpleStrategy', 'replication_factor':1};
create table if not exists cvs_copy_ks.kv (key int primary key, value text);
truncate cvs_copy_ks.kv;
copy cvs_copy_ks.kv (key, value) from 'kv.csv' with header='true' and numprocesses=1;
select * from cvs_copy_ks.kv;
drop keyspace cvs_copy_ks;
