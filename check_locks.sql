select pid,
       usename,
       pg_blocking_pids(pid) as blocked_by,
       query as blocked_query
from pg_stat_activity
where cardinality(pg_blocking_pids(pid)) > 0;

select pg_class.relname,
       pg_locks.mode
from pg_class,
     pg_locks
where pg_class.oid = pg_locks.relation
and pg_class.relnamespace >= 2200
;

select * from pg_stat_activity;

select nspname, relname, l.*
from pg_locks l
    join pg_class c on (relation = c.oid)
    join pg_namespace nsp on (c.relnamespace = nsp.oid)
where pid in (select pid
              from pg_stat_activity
              where datname = current_database()
                and query != current_query());
