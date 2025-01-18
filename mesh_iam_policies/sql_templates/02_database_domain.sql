use role sysadmin;

-- create the databases for each domains and environments
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
create or alter database {{ env.name }}_{{ domain.name }};
{% endfor -%}
{% endfor -%}
-- [end parallel block]
