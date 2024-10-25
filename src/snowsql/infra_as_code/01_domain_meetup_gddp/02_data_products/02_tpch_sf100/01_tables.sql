/*
CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.CUSTOMER (
    C_CUSTKEY    NUMBER,
    C_NAME       VARCHAR,
    C_ADDRESS    VARCHAR,
    C_NATIONKEY  NUMBER,
    C_PHONE      VARCHAR,
    C_ACCTBAL    NUMBER(15, 2),
    C_MKTSEGMENT VARCHAR,
    C_COMMENT    VARCHAR
)
ENABLE_SCHEMA_EVOLUTION=TRUE;

CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.LINEITEM (
    L_ORDERKEY       NUMBER,
    L_PARTKEY        NUMBER,
    L_SUPPKEY        NUMBER,
    L_LINENUMBER     NUMBER,
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
ENABLE_SCHEMA_EVOLUTION=TRUE;

CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.NATION (
    N_NATIONKEY NUMBER,
    N_NAME      VARCHAR,
    N_REGIONKEY NUMBER,
    N_COMMENT   VARCHAR
)
ENABLE_SCHEMA_EVOLUTION=TRUE;

CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.ORDERS (
    O_ORDERKEY      NUMBER,
    O_CUSTKEY       NUMBER,
    O_ORDERSTATUS   VARCHAR,
    O_TOTALPRICE    NUMBER(15, 2),
    O_ORDERDATE     DATE,
    O_ORDERPRIORITY VARCHAR,
    O_CLERK         VARCHAR,
    O_SHIPPRIORITY  NUMBER,
    O_COMMENT       VARCHAR
)
ENABLE_SCHEMA_EVOLUTION=TRUE;

CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.PARTSUPP (
    PS_PARTKEY     NUMBER,
    PS_SUPPKEY     NUMBER,
    PS_AVAILQTY    NUMBER,
    PS_SUPPLYCOST  NUMBER(15, 2),
    PS_COMMENT     VARCHAR
)
ENABLE_SCHEMA_EVOLUTION=TRUE;

CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.PART (
    P_PARTKEY      NUMBER,
    P_NAME         VARCHAR,
    P_MFGR         VARCHAR,
    P_BRAND        VARCHAR,
    P_TYPE         VARCHAR,
    P_SIZE         NUMBER,
    P_CONTAINER    VARCHAR,
    P_RETAILPRICE  NUMBER(15, 2),
    P_COMMENT      VARCHAR
)
ENABLE_SCHEMA_EVOLUTION=TRUE;

CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.REGION (
    R_REGIONKEY NUMBER,
    R_NAME      VARCHAR,
    R_COMMENT   VARCHAR
)
ENABLE_SCHEMA_EVOLUTION=TRUE;

CREATE OR ALTER TABLE &{data_domain}.TPCH_SF100.SUPPLIER (
    S_SUPPKEY   NUMBER,
    S_NAME      VARCHAR,
    S_ADDRESS   VARCHAR,
    S_NATIONKEY NUMBER,
    S_PHONE     VARCHAR,
    S_ACCTBAL   NUMBER(15, 2),
    S_COMMENT   VARCHAR
)
ENABLE_SCHEMA_EVOLUTION=TRUE;
*/
