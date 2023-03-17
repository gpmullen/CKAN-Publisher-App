set DATAPROVIDER_DB = '';
set DATAPROVIDER_SCHEMA = '';
create or replace stream control_spoke_stream on table $DATAPROVIDER_DB.$DATAPROVIDER_SCHEMA.CONTROL_SPOKE;

create or replace table published_spoke (
package_id string NULL
,database_name string not null
,schema_name string not null
,table_name string NOT NULL
,status string NOT NULL DEFAULT 'PENDING'
,resource_id string null
,last_updated_Date datetime_ltz default current_datetime());

