-- database roles are created with accoutadmin cauz' need securityadmin role
use role accountadmin;

-- create the database role "admin" for each domains and environments
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
create or alter database role {{ env.name }}_{{ domain.name }}.admin;
{% endfor -%}
{% endfor -%}
-- [end parallel block]

use role securityadmin;

-- grant the database role "admin" for each domains and environments to the "mesh_admin" role
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
grant database role {{ env.name }}_{{ domain.name }}.admin to role mesh_admin;
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- revoke ownership on future schemas from the database role "admin" to avoid errors
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
revoke ownership on future schemas in database {{ env.name }}_{{ domain.name }} from database role {{ env.name }}_{{ domain.name }}.admin;
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- re-apply ownership on future schemas to the database role "admin"
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
grant ownership on future schemas in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.admin copy current grants;
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- re-apply ownership on all schemas to the database role "admin"
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
grant ownership on all schemas in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.admin copy current grants;
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- revoke ownership on future schemas' objects from the database role "admin" to avoid errors
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for schema_object in managed_db_role_schema_objects -%}
revoke ownership on future {{ schema_object }} in database {{ env.name }}_{{ domain.name }} from database role {{ env.name }}_{{ domain.name }}.admin;
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- re-apply ownership on future schemas' objects from the database role "admin"
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for schema_object in managed_db_role_schema_objects -%}
grant ownership on future {{ schema_object }} in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.admin copy current grants;
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- re-apply ownership on all schemas' objects from the database role "admin"
-- grant on objects of type PIPE to DATABASE_ROLE are restricted.
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for schema_object in managed_db_role_schema_objects -%}
{% if schema_object | trim | lower != "pipes" -%}
grant ownership on all {{ schema_object }} in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.admin copy current grants;
{% endif -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]
