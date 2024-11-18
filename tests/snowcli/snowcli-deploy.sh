#!/bin/sh

# concat all sql files in $WORKSPACE_PATH/snowcli/ddls
# sorted by folders and names
cat `find $WORKSPACE_PATH/snowcli/ddls -iname '*.sql' | sort` | \
  snow sql \
    --account $SNOWFLAKE_ACCOUNT \
    --user $SNOWFLAKE_USER \
    --stdin \
    --variable "DOMAIN=MEETUP_GDDP" \
    --variable "S3_LANDING_ROLE_ARN=$S3_LANDING_ROLE_ARN" \
    --variable "S3_LANDING_BUCKET=$S3_LANDING_BUCKET" \
    --variable "S3_LAKE_ROLE_ARN=$S3_LAKE_ROLE_ARN" \
    --variable "S3_LAKE_BUCKET=$S3_LAKE_BUCKET" \
    --variable "S3_LAKE_EXTERNAL_ID=$S3_LAKE_EXTERNAL_ID" \
    --variable "GIT_REPO_URI=$GIT_REPO_URI" \
    --variable "GIT_REF=branches/""feature/setting_up_project\""
    #--authenticator externalbrowser
