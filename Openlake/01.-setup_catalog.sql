use role ACCOUNTADMIN;
use warehouse OPENLAKE_G2;
use database OPENLAKE;
use schema OPENLAKE.ETC;

--https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-vended-credentials#example-aws-glue
--1.- Create Catalog Integration
show catalog integrations;
create catalog integration if not exists int_cat_glue_rest
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
--  CATALOG_NAMESPACE = 'rest_catalog_integration'
  REST_CONFIG = (
    CATALOG_URI = '<catalog uri>'
    CATALOG_API_TYPE = AWS_GLUE
    CATALOG_NAME = '<aws account id>'
    --ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = '<iam role arn>'
    SIGV4_SIGNING_REGION = '<cloud region>'
  )
  REFRESH_INTERVAL_SECONDS = 60
  ENABLED = TRUE
;
grant all on integration int_cat_glue_rest to role sysadmin;

describe integration int_cat_glue_rest;
--Add "API_AWS_IAM_USER_ARN" to role Trust Relationship :"arn:aws>iam::..."
--Add "API_AWS_EXTERNAL_ID" to role Trust Relationship :"GH..."

SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('int_cat_glue_rest');
SELECT SYSTEM$LIST_NAMESPACES_FROM_CATALOG('int_cat_glue_rest');
SELECT SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG('int_cat_glue_rest', 'iceberg');



--2.1.- OR vended credentials
CREATE DATABASE OPENLAKE_ICE
  LINKED_CATALOG = (
    CATALOG = 'int_cat_glue_rest',
    ALLOWED_NAMESPACES = ('openlakeslv', 'openlakegld')
  );

SELECT SYSTEM$CATALOG_LINK_STATUS('OPENLAKE_ICE');



--2.2.-OR No vended credentials
create external volume if not exists vol_s3_openlake
    STORAGE_LOCATIONS = ((
         NAME = '<s3 bucket name>'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://<s3 bucket name>/OPENLAKE_ICE/' 
         STORAGE_AWS_ROLE_ARN = '<iam role arn>' 
         --STORAGE_AWS_EXTERNAL_ID = 'iceberg_volume_external_id'
    ))
    ALLOW_WRITES = TRUE
;
grant all on external volume vol_s3_openlake to role sysadmin;

DESC EXTERNAL VOLUME vol_s3_openlake;
--Add "STORAGE_AWS_EXTERNAL_ID" to role Trust Relationship :"GH..."
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('vol_s3_openlake');



--3.- Create Catalog-linked Database
use role sysadmin;
create or replace database OPENLAKE_ICE
    LINKED_CATALOG = (
        CATALOG = 'int_cat_glue_rest',
        NAMESPACE_MODE = FLATTEN_NESTED_NAMESPACE,
        ALLOWED_NAMESPACES = ('openlakeslv', 'openlakegld')
        NAMESPACE_FLATTEN_DELIMITER = '-'
        SYNC_INTERVAL_SECONDS = 60
    )
    EXTERNAL_VOLUME = 'vol_s3_openlake'
;

SELECT SYSTEM$CATALOG_LINK_STATUS('OPENLAKE_ICE');