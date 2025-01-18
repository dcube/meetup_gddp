-- -------------------------------------------------------------------------
-- working with metatata do not need any compute power
-- -------------------------------------------------------------------------
show tables in schema meetup_gddp.tpch_sf100;

-- -------------------------------------------------------------------------
-- time travel, clone, undrop
-- -------------------------------------------------------------------------
-- clone
select count(*) from meetup_gddp.tpch_sf100.customer; --15 000 000

-- oups i delete some data
delete from meetup_gddp.tpch_sf100.customer where c_nationkey < 6;
set del_query_id = (select last_query_id());

-- use time travel to query data as it was 5 minutes before
select count(*) from meetup_gddp.tpch_sf100.customer at(offset => -60*5);

-- use time travel to query data as it was before a statement
select count(*) from meetup_gddp.tpch_sf100.customer before(statement => $query_id);

-- clone the table
create table meetup_gddp.tpch_sf100.customer_tmp clone meetup_gddp.tpch_sf100.customer
    before(statement => $query_id);

-- swap table to revert to intial situation
alter table meetup_gddp.tpch_sf100.customer swap with meetup_gddp.tpch_sf100.customer_tmp;

select count(*) from meetup_gddp.tpch_sf100.customer;

drop table meetup_gddp.tpch_sf100.customer_tmp;

-- drop/undrop
use database snowflake;

drop database meetup_gddp;

undrop database meetup_gddp;

-- clone the entire meetup_gddp
create database meetup_gddp_20241211 clone meetup_gddp;

show tables in database meetup_gddp_20241211;

drop database meetup_gddp_20241211;

-- -------------------------------------------------------------------------
-- don't forget, working with metatata do not need any compute power
-- -------------------------------------------------------------------------
select * from table(result_scan('01b8c553-0001-410d-0000-f361000c4b66'));
