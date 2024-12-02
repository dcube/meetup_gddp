use role sysadmin;


-- time travel


-- clone
create database meetup_gddp_20241211_1 clone meetup_gddp;


-- time_travel + clone


-- drop / undrop
drop database meetup_gddp_20241211_1;

undrop database meetup_gddp_20241211_1;


-- unload
