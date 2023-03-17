CREATE OR REPLACE PROCEDURE SP_PUBLISH_SPOKE()
RETURNS VARIANT
LANGUAGE SQL
AS 
BEGIN


    
    //make the api call. Updates packageid
    Insert into published_spoke
    (package_id,database_name,schema_name, table_name,status) 
    select            
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
      , 'PACKAGE CREATED'
      from CONTROL_SPOKE_STREAM      
    WHERE METADATA$ISUPDATE = FALSE
    AND METADATA$ACTION <> 'DELETE';
    


//This makes another API call which stores the URL as a Resource
                                                         
update published_spoke
    set resource_id = ext_resource_id
    ,status = 'PUBLISHED'
    ,last_updated_date = CURRENT_TIMESTAMP()
FROM (
    WITH CS AS (select database_name, schema_name, table_name, notes, presigned_url 
            FROM OPENDATA_WATERBOARDS_DB.OPENDATA.CONTROL_SPOKE)           
    select resource_create(ps.package_id,lower(ps.table_name),cs.notes,'CSV'
                       ,(cs.presigned_url)) ext_resource_id
      , ps.package_id

    from published_spoke PS
    INNER JOIN CS
        on PS.database_name = CS.database_name
        and PS.schema_name = CS.schema_name
        and PS.table_name = CS.table_name
    WHERE ps.status = 'PACKAGE CREATED') SPOKE
    ;

    set LQID = LAST_QUERY_ID();


execute immediate ('insert into ckan_log select current_timestamp(),package_id,table_name from published_spoke BEFORE(STATEMENT => ''' 
|| $LQID::string || ''') WHERE status = ''PACKAGE CREATED''');
    
exception
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    insert into ckan_log select localtimestamp(), 'ERROR', :err::string;
END;

