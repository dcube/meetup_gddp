--!jinja

------------------------------------------------------------------------------
-- create tables for tpch
------------------------------------------------------------------------------

-- create snowflake internal table or iceberg tables
-- depending on the schema name
{% set create_stmt = "CREATE TABLE" %}
{% if schema == "TPCH_SF100_ICEBERG" %}
    {% set create_stmt = "CREATE ICEBERG TABLE" %}
{% endif %}

-- create each tables
{{ create_stmt }} IF NOT EXISTS {{ schema }}.CUSTOMER (
    C_CUSTKEY    BIGINT,
    C_NAME       VARCHAR,
    C_ADDRESS    VARCHAR,
    C_NATIONKEY  INT,
    C_PHONE      VARCHAR,
    C_ACCTBAL    NUMBER(15, 2),
    C_MKTSEGMENT VARCHAR,
    C_COMMENT    VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/CUSTOMER'
{% endif %}
;

{{ create_stmt }} IF NOT EXISTS {{ schema }}.LINEITEM (
    L_ORDERKEY       BIGINT,
    L_PARTKEY        BIGINT,
    L_SUPPKEY        BIGINT,
    L_LINENUMBER     BIGINT,
    L_QUANTITY       NUMBER(15, 2),
    L_EXTENDEDPRICE  NUMBER(15, 2),
    L_DISCOUNT       NUMBER(15, 2),
    L_TAX            NUMBER(15, 2),
    L_RETURNFLAG     VARCHAR,
    L_LINESTATUS     VARCHAR,
    L_SHIPDATE       DATE,
    L_COMMITDATE     DATE,
    L_RECEIPTDATE    DATE,
    L_SHIPINSTRUCT   VARCHAR,
    L_SHIPMODE       VARCHAR,
    L_COMMENT        VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/LINEITEM'
{% endif %}
;

{{ create_stmt }} IF NOT EXISTS {{ schema }}.NATION (
    N_NATIONKEY INT,
    N_NAME      VARCHAR,
    N_REGIONKEY INT,
    N_COMMENT   VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/NATION'
{% endif %}
;

{{ create_stmt }} IF NOT EXISTS {{ schema }}.ORDERS (
    O_ORDERKEY      BIGINT,
    O_CUSTKEY       BIGINT,
    O_ORDERSTATUS   VARCHAR,
    O_TOTALPRICE    NUMBER(15, 2),
    O_ORDERDATE     DATE,
    O_ORDERPRIORITY VARCHAR,
    O_CLERK         VARCHAR,
    O_SHIPPRIORITY  INT,
    O_COMMENT       VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/ORDERS'
{% endif %}
;

{{ create_stmt }} IF NOT EXISTS {{ schema }}.PARTSUPP (
    PS_PARTKEY     BIGINT,
    PS_SUPPKEY     BIGINT,
    PS_AVAILQTY    BIGINT,
    PS_SUPPLYCOST  NUMBER(15, 2),
    PS_COMMENT     VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/PARTSUPP'
{% endif %}
;

{{ create_stmt }} IF NOT EXISTS {{ schema }}.PART (
    P_PARTKEY      BIGINT,
    P_NAME         VARCHAR,
    P_MFGR         VARCHAR,
    P_BRAND        VARCHAR,
    P_TYPE         VARCHAR,
    P_SIZE         INT,
    P_CONTAINER    VARCHAR,
    P_RETAILPRICE  NUMBER(15, 2),
    P_COMMENT      VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/PART'
{% endif %}
;

{{ create_stmt }} IF NOT EXISTS {{ schema }}.REGION (
    R_REGIONKEY INT,
    R_NAME      VARCHAR,
    R_COMMENT   VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/REGION'
{% endif %}
;

{{ create_stmt }} IF NOT EXISTS {{ schema }}.SUPPLIER (
    S_SUPPKEY   BIGINT,
    S_NAME      VARCHAR,
    S_ADDRESS   VARCHAR,
    S_NATIONKEY INT,
    S_PHONE     VARCHAR,
    S_ACCTBAL   NUMBER(15, 2),
    S_COMMENT   VARCHAR
)
{% if schema == "TPCH_SF100_ICEBERG" %}
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{{ domain }}_S3_LAKEHOUSE'
BASE_LOCATION = '{{ schema }}/SUPPLIER'
{% endif %}
;
