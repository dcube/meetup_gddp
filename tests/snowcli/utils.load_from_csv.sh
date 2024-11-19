  snow sql \
    --account $SNOWFLAKE_ACCOUNT \
    --user $SNOWFLAKE_USER \
    --warehouse MANAGE \
    --query "CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ \"table_name\": \"MEETUP_GDDP.TPCH_SF100.REGION\", \"stage_path\": \"@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/region/\", \"file_format\": \"MEETUP_GDDP.UTILS.CSV_FMT1\", \"mode\": \"truncate\", \"force\": true}'));"
