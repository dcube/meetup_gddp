#!/bin/sh

# log on to azure to get secrets from key vault
az login --tenant $AZURE_TENANT
az account set --subscription $AZURE_SUBSCRIPTION

# get secrets
S3_LANDING_ROLE_ARN=$(az keyvault secret show --vault-name $AZURE_KEYVAULT --name "s3-landing-role-arn" --query value -o tsv)
S3_LANDING_BUCKET=$(az keyvault secret show --vault-name $AZURE_KEYVAULT --name "s3-landing-bucket" --query value -o tsv)
S3_LAKE_ROLE_ARN=$(az keyvault secret show --vault-name $AZURE_KEYVAULT --name "s3-lake-role-arn" --query value -o tsv)
S3_LAKE_BUCKET=$(az keyvault secret show --vault-name $AZURE_KEYVAULT --name "s3-lake-bucket" --query value -o tsv)
S3_LAKE_EXTERNAL_ID=$(az keyvault secret show --vault-name $AZURE_KEYVAULT --name "s3-lake-external-id" --query value -o tsv)

# concat all sql files in $WORKSPACE_PATH/snowcli/ddls
# sorted by folders and names
cat `find $WORKSPACE_PATH/snowcli/ddls -iname '*.sql' | sort` | \
  snow sql \
    --account $SNOWFLAKE_ACCOUNT \
    --user $SNOWFLAKE_USER \
    --warehouse MANAGE \
    --stdin \
    --variable "domain=MEETUP_GDDP" \
    --variable "s3_landing_role_arn=$S3_LANDING_ROLE_ARN" \
    --variable "s3_landing_bucket=$S3_LANDING_BUCKET" \
    --variable "s3_lake_role_arn=$S3_LAKE_ROLE_ARN" \
    --variable "s3_lake_bucket=$S3_LAKE_BUCKET" \
    --variable "s3_lake_external_id=$S3_LAKE_EXTERNAL_ID" \
    --variable "git_repo_uri=$GIT_REPO_URI" \
    --variable "git_ref=branches/\"feature/setting_up_project\"" \
    --variable "dry_run=FALSE";
    #--authenticator externalbrowser
