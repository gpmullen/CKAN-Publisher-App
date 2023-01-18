--RESOURCE UPDATE

    create or replace external function resource_update(id varchar,format string
        ,url string)
    returns variant
    api_integration = ckan_proxy_int
    MAX_BATCH_ROWS = 1
        request_translator = CKAN_resource_update_request_translator
        response_translator = CKAN_resource_update_response_translator
    as 'https://[endpoint copied from api integration]/resource_update';
    

    create or replace function CKAN_resource_update_request_translator(event object)
    returns object
    language javascript as
    '
    var resource_id;
    var format ; 
    var url ; 

       let row = EVENT.body.data[0];
       resource_id = row[1]
       format=row[2]; 
       url=row[3];     

    return { "body": { "id": resource_id, "format" : format, "url" : url } }
    ';    
    
    create or replace function CKAN_resource_update_response_translator(event object)
    returns object
    language javascript as
    '
    var responses = new Array(0);
    responses[0] = [0,EVENT.body.result.id]
    return { "body": { "data" : responses } };
    ';