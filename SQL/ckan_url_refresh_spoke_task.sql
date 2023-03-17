
CREATE OR REPLACE TASK CKAN_URL_REFRESH_spoke
warehouse ='OPEN_DATA_VWH'
SCHEDULE = '1440 MINUTE' //1 DAY
AS
BEGIN
    insert into ckan_log select localtimestamp()
     ,resource_update(resource_id,'CSV',(get_presigned_url(@published_extracts, table_name || '.csv',604800))) ext_resource_id
     ,'REFRESH: ' || table_name
    from control_spoke
    where status='PUBLISHED';

    insert into ckan_log select localtimestamp(), 'SCHEDULED REFRESH', 'COMPLETE';
exception
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    insert into ckan_log select localtimestamp(), 'ERROR', :err::string;    
END;

alter task ckan_url_refresh resume;

execute task CKAN_URL_REFRESH;