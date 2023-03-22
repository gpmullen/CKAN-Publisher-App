set DATAPROVIDER_DB ='';
set DATAPROVIDER_SCHEMA='';

CREATE OR REPLACE TASK CKAN_URL_REFRESH_SPOKE
warehouse ='OPEN_DATA_VWH'
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('control_spoke_stream')
AS
BEGIN
    insert into ckan_log select localtimestamp()
     ,resource_update(ps.resource_id,'CSV',css.presigned_url) ext_resource_id
     ,'REFRESH: ' || ps.table_name
    from control_spoke_stream css
    INNER JOIN published_spoke ps
        on css.database_name = ps.database_name
        and css.schema_name = ps.schema_name
        and css.table_name = ps.table_name
    WHERE css.METADATA$ACTION = 'INSERT' 
    AND css.METADATA$ISUPDATE = 'TRUE';

    insert into ckan_log select localtimestamp(), 'SCHEDULED SPOKE REFRESH', 'COMPLETE';
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