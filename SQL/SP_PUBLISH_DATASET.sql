CREATE OR REPLACE PROCEDURE SP_PUBLISH_DATASET()
RETURNS VARIANT
LANGUAGE SQL
AS
DECLARE
    TABLES RESULTSET DEFAULT(select database_name, schema_name, table_name, database_name||'.'||schema_name||'.'||table_name FQTN from control_stream);
    ret variant default '{}';   
BEGIN

    FOR tbl IN tables DO
        //drop all published files to internal stage
        execute immediate ( 'copy into @published_extracts/' ||
        tbl.table_name || '.csv from ' ||
        tbl.FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE file_format = (TYPE = csv COMPRESSION = none FIELD_OPTIONALLY_ENCLOSED_BY=''\042'');');
    END FOR;

    
    //make the api call. Updates packageid
    UPDATE CONTROL 
        set package_id = EXT_PACKAGE_ID 
    FROM (select            
      package_create(lower(database_name||'_'||schema_name||'_'||table_name)
                     ,notes
                     ,accesslevel
                     ,contact_name
                     ,contact_email,rights  
                     ,accrualperiodicity
                     ,tag_string  
                     ,owner_org) EXT_PACKAGE_ID
      , database_name
      , schema_name
      , table_name
      , METADATA$ISUPDATE ISUPDATE
      , METADATA$ACTION ACTION
      from control_Stream) STRM 
      
    WHERE CONTROL.TABLE_NAME = STRM.TABLE_NAME 
    AND CONTROL.DATABASE_NAME = STRM.DATABASE_NAME
    AND CONTROL.SCHEMA_NAME = STRM.SCHEMA_NAME
    AND ISUPDATE = FALSE
    AND ACTION <> 'DELETE';
    


//This makes another API call which stores the URL as a Resource
                                                                    
update control
    set resource_id = ext_resource_id
    ,status = 'PUBLISHED'
FROM (
    select resource_create(package_id,lower(table_name),notes,'CSV'
                       ,(get_presigned_url(@published_extracts, table_name || '.csv',604800))) ext_resource_id
      , package_id
      , METADATA$ISUPDATE ISUPDATE
      , METADATA$ACTION ACTION
                       from control_stream 
                       WHERE METADATA$ACTION = 'INSERT' 
                       AND METADATA$ISUPDATE = TRUE) STRM
WHERE CONTROL.PACKAGE_ID = STRM.PACKAGE_ID
AND ACTION = 'INSERT'
AND ISUPDATE = 'TRUE';

insert into ckan_log 
select current_timestamp(),package_id,table_name 
from control_Stream;
    
exception
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    insert into ckan_log select localtimestamp(), 'ERROR', :err::string from control_Stream;
END;

