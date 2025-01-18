-- Create the dedicated role "mesh_admin" to manage the data mesh
use role securityadmin;
create or alter role mesh_admin;
grant role mesh_admin to role sysadmin;
