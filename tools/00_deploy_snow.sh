#!/bin/bash
cd $WORKSPACE_PATH/src/snowsql/infra_as_code/
cat `find . -iname '*.sql' | sort` | snow sql \
  --account $SNOWFLAKE_ACCOUNT \
  --user $SNOWFLAKE_USER \
  --variable s3_aws_landing_role_arn=$S3_AWS_LANDING_ROLE_ARN \
  --variable s3_bucket_landing=$S3_BUCKET_LANDING \
  --variable s3_aws_lake_role_arn=$S3_AWS_LAKE_ROLE_ARN \
  --variable s3_bucket_lake=$S3_BUCKET_LAKE \
  --variable s3_aws_lake_external_id=$S3_AWS_LAKE_EXTERNAL_ID \
  --variable git_repo_uri=$GIT_REPO_URI \
  --variable data_domain=MEETUP_GDDP \
  --variable owners_grants_future_all=ALL \
  --variable branch=feature/setting_up_project \
  --stdin \
  --verbose
