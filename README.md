# data mesh with Snowflake
Ease your snowflake objects management in a data mesh context.
This tools allows you to:
- manage your storage urbanization: create databases' domain by environments, data zones or any other discriminant field you want
- create / alter / replace the databases' roles (admin, operate, read, write, etc.) and manage their privileges on schemas and schemas' objects
- create / alter the domain roles to define and manage the different domains' personaes (data ops, data engineers, data analysts, data scientists, etc.)

The data mesh structure is set and define into the **project_config.yml** file descibed further.

The **data_mesh_manager.py** console app is used to :
- plan: aka parse the yaml file and generate blocks of sql statements to create / alter or replace the snowflake objects
- and apply: run the sql statements' blocks

## Prepare your Snowflake account
- Create the MESH_ADMIN role that will be used to manage all of your data mesh objects
It must be created with the SECURITYADMIN role and will be granted to the ACCOUNTADMIN role.
```sql
use role securityadmin;
create role mesh_admin;
grant role securityadmin to role mesh_admin;
grant role mesh_admin to role accountadmin
```
- Create the MESH_DEVOPS user that will be used to manage your data mesh objects, including the databases' domains, their roles and privileges, and the domain roles along with their respective database roles and privileges.
This user will be use as a service account by your devops tool to manage and deploy your data mesh triggered by pull request
```sql
use role securityadmin;
create user mesh_devops type='legacy_service';
grant role mesh_admin to user mesh_devops;
```
And configure a key-pair authentication for this user. Please refer to the [key-pair authentication Snowflake documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth).
**Note that the private key and passphrase must be stored in a secret store** such as Azure Key Vault, AWS Secret Manager or GCP Secret Manager.


- Feel free to grant the MESH_ADMIN role to any other user. Be careful not to grant this right to too many users (5 to 10 people maximum).
```sql
use role securityadmin;
grant role mesh_admin to user <user_person_a>;
```

## Set the project_config.yml



## Run the data_mesh_manager.py
