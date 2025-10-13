use role SYSADMIN;
use warehouse OPENLAKE_G2;

create database OPENLAKE;
create schema OPENLAKE.GOV; --tags, policies, etc
create schema OPENLAKE.BLD; --build pipelines
create schema OPENLAKE.ETC; --unspecified artifacts

create database OPENLAKE_RAW;
create schema OPENLAKE_RAW.REP_EHUB001;

create database OPENLAKE_SRV; --managed tables serving layer
--create database OPENLAKE_ICE; --catalog linked database