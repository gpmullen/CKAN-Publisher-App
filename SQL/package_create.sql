--package create

    create or replace external function package_create(
        name string
        ,notes string 
        ,accesslevel string 
        ,contact_name string 
        ,contact_email string 
        ,rights string 
        ,accrualperiodicity string 
        ,tag_string string 
        ,owner_org string )
    returns variant
    api_integration = ckan_proxy_int
    MAX_BATCH_ROWS = 1
        request_translator = CKAN_package_create_request_translator
        response_translator = CKAN_package_create_response_translator
    as 'https://[endpoint copied from api integration]/package_create';
    
    

     create or replace function CKAN_package_create_request_translator(event object)
    returns object
    language javascript as
    '
    var name;
    var notes ; 
    var accesslevel ; 
    var contact_name ; 
    var contact_email ; 
    var rights ; 
    var accrualPeriodicity ; 
    var tag_string  ;
    var owner_org  ;
   
       let row = EVENT.body.data[0];
       name = row[1];
       notes=row[2]; 
       accesslevel=row[3]; 
       contact_name=row[4]; 
       contact_email=row[5]; 
       rights=row[6]; 
       accrualPeriodicity=row[7]; 
       tag_string=row[8]; 
       owner_org=row[9]; 
    

    return { "body": { "name": name, "notes" : notes, "accessLevel" : accesslevel, "contact_name" : contact_name, "contact_email" : contact_email, "rights" : rights, 
    "accrualPeriodicity" : accrualPeriodicity, "tag_string" : tag_string, "owner_org" : owner_org } }
    ';    
    create or replace function CKAN_package_create_response_translator(event object)
    returns object
    language javascript as
    '
    var responses = new Array(0);
    responses[0] = [0,EVENT.body.result.id]
    return { "body": { "data" : responses } };
    ';