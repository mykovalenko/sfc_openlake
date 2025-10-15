use warehouse OPENLAKE_G2;
use database OPENLAKE_ICE;
create schema if not exists OPENLAKE_ICE."openlakeslv";
use schema OPENLAKE_ICE."openlakeslv";

drop table if exists "s_accounts";
create iceberg table "s_accounts" (
	"account_id" decimal(38,0),
	"customer_id" decimal(38,0),
	"account_type" string,
	"balance" float,
	"open_date" date,
    "updated_at" timestamp,
    "ref_pk" decimal(38,0)
);

insert into "s_accounts"
select "account_id", "customer_id",	"account_type",	"balance", "open_date"
from OPENLAKE_RAW."rep_psgs"."accounts" limit 10;

select * from "s_accounts";



drop table if exists "s_customers";
create iceberg table "s_customers" (
    "customer_id" decimal(38,0),
    "first_name" string,
    "last_name" string,
    "email" string,
    "phone_number" string,
    "address" string,
    "state" string,
    "creation_date" date,
    "updated_at" timestamp,
    "ref_pk" decimal(38,0)
);

insert into "s_customers"
select "customer_id", "first_name", "last_name", "email", "phone_number", "address", "state", "creation_date"
from OPENLAKE_RAW."rep_psgs"."customers" limit 10;

select * from "s_customers";



drop table if exists "s_transactions";
create iceberg table "s_transactions" (
    "transaction_id" decimal(38,0),
    "account_id" decimal(38,0),
    "transaction_date" timestamp,
    "amount" float,
    "description" string,
    "updated_at" timestamp,
    "ref_pk" decimal(38,0)
);

insert into "s_transactions"
select "transaction_id", "account_id", "transaction_date", "amount", "description"
from OPENLAKE_RAW."rep_psgs"."transactions" limit 10;

select * from "s_transactions";




drop table if exists "s_loans";
create iceberg table "s_loans" (
    "loan_id" decimal(38,0),
    "customer_id" decimal(38,0),
    "loan_amount" float,
    "interest_rate" float,
    "status" string,
    "start_date" date,
    "updated_at" timestamp,
    "ref_pk" decimal(38,0)
);

insert into "s_loans"
select "loan_id", "customer_id", "loan_amount", "interest_rate", "status", "start_date"
from OPENLAKE_RAW."rep_psgs"."loans" limit 10;

select * from "s_loans";



drop table if exists "s_credit_cards";
create iceberg table "s_credit_cards" (
    "card_id" decimal(38,0),
    "customer_id" decimal(38,0),
    "card_type" string,
    "credit_limit" decimal(38,0),
    "current_balance" float,
    "updated_at" timestamp,
    "ref_pk" decimal(38,0)
);

insert into "s_credit_cards"
select "card_id", "customer_id", "card_type", "credit_limit", "current_balance"
from OPENLAKE_RAW."rep_psgs"."credit_cards" limit 10;

select * from "s_credit_cards";




drop table if exists "s_customer_interactions";
create iceberg table "s_customer_interactions" (
    "interaction_id" decimal(38,0),
    "customer_id" decimal(38,0),
    "interaction_date" timestamp,
    "channel" string,
    "notes" string,
    "updated_at" timestamp,
    "ref_pk" decimal(38,0)
);

insert into "s_customer_interactions"
select "interaction_id", "customer_id", "interaction_date", "channel", "notes"
from OPENLAKE_RAW."rep_psgs"."customer_interactions" limit 10;

select * from "s_customer_interactions";


/*-- Not supported by external catalog
create or replace iceberg table "accounts_tmp" as
select "account_id", "customer_id",	"account_type",	"balance", "open_date"
from openlake_raw."rep_sqls"."accounts";

create or replace iceberg table "accounts_tmp"; 
    EXTERNAL_VOLUME = 'VOL_S3_OPENLAKE'
    CATALOG = 'INT_CAT_GLUE_REST'
    CATALOG_TABLE_NAME = 'accounts'
    CATALOG_NAMESPACE = 'openlakeslv';
*/
