--RESOURCE CREATE

    create or replace external function resource_create(
        package_id string
        ,name string
        ,description string 
        ,format string
        ,url string)
    returns variant
    api_integration = ckan_proxy_int
    MAX_BATCH_ROWS = 1
        request_translator = CKAN_resouce_create_request_translator
        response_translator = CKAN_resource_create_response_translator
    as 'https://[endpoint copied from api integration]/resource_create';
    

    create or replace function CKAN_resouce_create_request_translator(event object)
    returns object
    language javascript as
    '
    var package_id;
    var name ; 
    var description ; 
    var format ; 
    var url ; 

       let row = EVENT.body.data[0];
       package_id = row[1]
       name=row[2]; 
       description=row[3]; 
       format=row[4]; 
       url=row[5];     

    return { "body": { "package_id": package_id, "name" : name, "description" : description, "format" : format, "url" : url } }
    ';    
    
create or replace function CKAN_resource_create_response_translator(event object)
    returns object
    language javascript as
    '
    var responses = new Array(0);
    responses[0] = [0,EVENT.body.result.id]
    return { "body": { "data" : responses } };
    ';
           
                                                                     
