/*-execute-*/ 

-------->Adding new partitioning in Redshift Spectrum for the new batch
alter table spectrum.events_ext
add partition(batch=':env_BATCHID') 
location 's3://glue.sneaql.full360.com/beers-parquet/batch=:env_BATCHID/';

--------->Passing aggregate data from Spectrum to Redshift temp table
drop table if exists events_by_user_temp;
create temp table events_by_user_temp
as
select
    user_id
    ,min(distinct beer_opened_time) as earliest_beer_opened_time
    ,max(distinct beer_opened_time) as latest_beer_opened_time
    ,count(*) as total_beers_opened
from
    spectrum.events_ext
where
    batch = ':env_BATCHID'
group by user_id;

------->For testing the demo (archieving aggregated data for each batch)
insert into beer.events_by_user_history
select * from events_by_user_temp;
update beer.events_by_user_history
set batch = ':env_BATCHID' where batch is null;


--------->Identifying/Preparing data that only needs to be updated or inserted 
drop table if exists final_updatable_users;
create temp table final_updatable_users as
select
  u.user_id,
  least (u.earliest_beer_opened_time, e.earliest_beer_opened_time) as earliest_beer_opened_time,
  greatest (u.latest_beer_opened_time, e.latest_beer_opened_time) as latest_beer_opened_time ,
  u.total_beers_opened + nvl(e.total_beers_opened,0) as total_beers_opened
from 
  events_by_user_temp u
  left join
  beer.events_by_user e
    on
      u.user_id=e.user_id;

--------->Removing data from Target table that needs to inserted with latest updates.
delete from  beer.events_by_user where user_id in (select user_id from final_updatable_users);

--------->Finally upserting the latest and greatest data to the target table.
insert into beer.events_by_user select * from final_updatable_users;


--------> testing if upsert succeeded
/*-assign_result spectrum_count-*/
select count(*) from spectrum.events_ext;

/*-test = :spectrum_count-*/
select sum(total_beers_opened) from events_by_user_temp;




