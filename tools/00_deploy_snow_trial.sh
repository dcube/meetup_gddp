#!/bin/bash
cd $WORKSPACE_PATH/src/admin/
cat `find . -iname '*.sql' | sort` | snowsql -a SC20860.eu-west-3.aws -u FREDBROSSARD -D s3_aws_role_arn=$S3_AWS_ROLE_ARN -D s3_bucket_landing=$S3_BUCKET_LANDING -D s3_bucket_lake=$S3_BUCKET_LAKE -D git_repo_uri=$GIT_REPO_URI
