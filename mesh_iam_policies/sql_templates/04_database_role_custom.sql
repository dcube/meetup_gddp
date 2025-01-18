-- Create database roles custom for each environment, layer and domain dbs
use role accountadmin;

-- create the database roles for each domains and environments
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for role in database_roles -%}
{% if role | trim | lower != "admin" -%}
create or replace database role {{ env.name }}_{{ domain.name }}.{{ role.name }};
{% endif -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]

use role securityadmin;
-- apply schema privleges on future schemas to databases roles
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for role in database_roles -%}
{% for privilege in role.schemas_privileges -%}
{% if privilege | trim | lower != "ownership" -%}
grant {{ privilege }} on future schemas in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.{{ role.name }};
{% endif -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- apply schema privileges on all schemas to databases roles
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for role in database_roles -%}
{% for privilege in role.schemas_privileges -%}
{% if privilege | trim | lower != "ownership" -%}
grant {{ privilege }} on all schemas in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.{{ role.name }};
{% endif -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- apply schema objects privileges on future objects to databases roles
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for role in database_roles -%}
{% for schema_object, privilege in role.schema_objects_privileges.items() -%}
{% if privilege | trim | lower != "ownership" -%}
grant {{ privilege | map('trim') | map('lower') | reject("equalto", "ownership") | join(", ") }} on future {{ schema_object }} in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.{{ role.name }};
{% endif -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]

-- apply schema objects privileges on all objects to databases roles
-- [start parallel block]
{% for domain in domains -%}
{% for env in environments -%}
{% for role in database_roles -%}
{% for schema_object, privilege in role.schema_objects_privileges.items() -%}
{% if privilege | trim | lower != "ownership" and schema_object | trim | lower != "pipes" -%}
grant {{ privilege | map('trim') | map('lower') | reject("equalto", "ownership") | join(", ") }} on all {{ schema_object }} in database {{ env.name }}_{{ domain.name }} to database role {{ env.name }}_{{ domain.name }}.{{ role.name }};
{% endif -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}
-- [end parallel block]
