set DATAPROVIDER_DB ='';
set DATAPROVIDER_SCHEMA='';
create or replace stream resource_stream on table IDENTIFIER($DATAPROVIDER_DB).IDENTIFIER($DATAPROVIDER_SCHEMA).RESOURCES;

CREATE OR REPLACE TASK CKAN_URL_REFRESH_RESOURCE_TASK
warehouse ='OPEN_DATA_VWH'
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('resource_stream')
AS
BEGIN
    insert into ckan_log select localtimestamp()
     ,resource_update(rs.resource_id,'CSV',rs.presigned_url) ext_resource_id
     ,'REFRESH: ' || rs.table_name
    from resource_stream rs
    WHERE rs.METADATA$ACTION = 'INSERT';

    insert into ckan_log select localtimestamp(), 'SCHEDULED SPOKE RESOURCE REFRESH', 'COMPLETE';
exception
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    insert into ckan_log select localtimestamp(), 'ERROR', :err::string;    
END;

alter task CKAN_URL_REFRESH_SPOKE resume;

execute task CKAN_URL_REFRESH_SPOKE;