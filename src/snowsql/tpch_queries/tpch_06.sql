-- TPC-H 6
select
	sum(l_extendedprice * l_discount) as revenue
from
	snowflake_sample_data.tpch_sf1.lineitem
where
	l_shipdate >= date '1994-01-01'
	AND l_shipdate < DATEADD(year, 1, '1994-01-01')
	and l_discount between .06 - 0.01 and .06 + 0.01
	and l_quantity < 24;
