#!/bin/sh

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
    --variable "git_ref=branches/\"feature/setting_up_project\""
    #--authenticator externalbrowser
