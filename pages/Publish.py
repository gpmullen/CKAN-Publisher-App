import streamlit as st
from snowflake.snowpark import Session
import re
    
def getDatabases():
    if 'tables' in st.session_state:
        return set([row[2] for row in st.session_state.tables])

def getSchemas():
    if 'ddlDatabaseToPublish' in st.session_state:
        return set([row[3] for row in st.session_state.tables if row[2] == st.session_state.ddlDatabaseToPublish])
    else:
        return []

def getTables():
    if 'ddlSchemaToPublish' in st.session_state:
        return [row[1] for row in st.session_state.tables if row[3] == st.session_state.ddlSchemaToPublish and row[2] == st.session_state.ddlDatabaseToPublish]
    else:
        return []

def mapFrequency(freq):
    if freq == 'Irregular':
        return 'irregular'
    elif freq == 'Continuously updated':
        return 'R/PT1S'
    elif freq == 'Hourly':
        return 'R/P1H'
    elif freq == 'Daily':
        return 'R/P1D'
    elif freq == 'Twice a week':
        return 'R/P0.5W'
    elif freq == 'Semiweekly':
        return 'R/P3.5D'
    elif freq == 'Biweekly':
        return 'R/P1W'
    elif freq == 'Semimonthly':
        return 'R/P2W'
    elif freq == 'Monthly':
        return 'R/P1M'
    elif freq == 'Every Two Months':
        return 'R/P2M'
    elif freq == 'Quarterly':
        return 'R/P3M'
    elif freq == 'Semiannual':
        return 'R/P6M'
    elif freq == 'Biennial':
        return 'R/P2Y'
    elif freq == 'Decennial':
        return 'R/P10Y'
    
def refresh():
       del st.session_state.tables

def publishTable():
    if "txtDesc" in st.session_state: txtDesc=st.session_state["txtDesc"] 
    if "ddlAccessLevel" in st.session_state: ddlAccessLevel=st.session_state["ddlAccessLevel"] 
    if "txtContactName" in st.session_state: txtContactName=st.session_state["txtContactName"] 
    if "txtContactEmail" in st.session_state: txtContactEmail=st.session_state["txtContactEmail"] 
    if "txtRights" in st.session_state: txtRights=st.session_state["txtRights"] 
    if "ddlFrequency" in st.session_state: ddlFrequency=st.session_state["ddlFrequency"] 
    if "txtTags" in st.session_state: txtTags=st.session_state["txtTags"] 
    if "txtOwnerOrg" in st.session_state: txtOwnerOrg=st.session_state["txtOwnerOrg"] 
    if "ddlDatabaseToPublish" in st.session_state: ddlDatabaseToPublish=st.session_state["ddlDatabaseToPublish"] 
    if "ddlSchemaToPublish" in st.session_state: ddlSchemaToPublish=st.session_state["ddlSchemaToPublish"] 
    if "ddlTableToPublish" in st.session_state: ddlTableToPublish=st.session_state["ddlTableToPublish"] 
    status = 'PENDING'
    valid_email_regex = "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    compiled_regex = re.compile(valid_email_regex)

    if txtDesc and ddlAccessLevel and txtContactEmail and txtContactName and txtRights \
    and ddlFrequency and txtTags and txtOwnerOrg and ddlDatabaseToPublish and ddlSchemaToPublish \
    and ddlTableToPublish and compiled_regex.match(txtContactEmail):
        session = Session.builder.configs(st.session_state.connection_parameters).create()        #Fully Qualified Table Name        
        dfControl = session.create_dataframe([[None,txtDesc.lower(),ddlAccessLevel.lower(),txtContactName.lower(),txtContactEmail,txtRights,mapFrequency(ddlFrequency),txtTags,txtOwnerOrg,ddlDatabaseToPublish,ddlSchemaToPublish,ddlTableToPublish,status,None]])#,schema=["Notes","Access_Level","Contact_Name","Contact_Email","Rights","Accural_Periodicity","Tags","Owner_Org"])
        #insert
        dfControl.write.mode("append").save_as_table("{0}.{1}.{2}".format(Control_DB,Control_Schema,Control_Table))
        session.sql("call SP_PUBLISH_DATASET()").collect()
        session.close()
        st.success('Saved!', icon="✅")
    else:      
        if not txtDesc:
            st.info('Description field is empty')
        if not ddlAccessLevel:
            st.info('Access Level is empty')
        if not txtContactName:
            st.info('Name is empty')
        if not txtContactEmail:
            st.info('Email is empty')  
        elif not compiled_regex.match(txtContactEmail) :  
            st.error('Not a valid email: {0}'.format(txtContactEmail))
        if not txtRights:
            st.info('Rights is empty')
        if not ddlFrequency:
            st.info('Frequency is empty')
        if not txtTags:
            st.info('Tags is empty')
        if not txtOwnerOrg:
            st.info('Owner Org is empty')

if 'connection_parameters' not in st.session_state:
    st.error('Set Context first!')
else:

    Control_DB = st.session_state.connection_parameters['database']
    Control_Schema = st.session_state.connection_parameters['schema']
    Control_Table = 'CONTROL'
    st.info("Control table is currently set to {0}.{1}.{2}".format(Control_DB,Control_Schema,Control_Table))

    if 'tables' not in st.session_state:
        session = Session.builder.configs(st.session_state.connection_parameters).create()
        st.session_state.tables = session.sql('SHOW TABLES IN ACCOUNT').collect()
        #next line is a little convoluted because the columns are slightly different and doing it this way avoids additional permission on the snowflake database to get the same information
        st.session_state.tables.extend(session.sql('BEGIN SHOW VIEWS IN ACCOUNT;let ret resultset := (SELECT "created_on","name","database_name","schema_name" FROM TABLE (result_scan(LAST_QUERY_ID()))); return table(ret);END;').collect())
        # 1 = Tables
        # 2 = Database
        # 3 = Schema
        session.close()

    st.info('Choose a table to publish. All metadata must be populated.')
    col1, col2 = st.columns(2)
    with col1:
        txtDesc = st.text_input("Description", help='Required', key='txtDesc')
        ddlAccessLevel = st.selectbox("Access Level",options=('Public','Restricted','Non-public'), help='Required', key='ddlAccessLevel')
        txtContactName = st.text_input("Contact Name", help='Required', key='txtContactName')
        txtContactEmail = st.text_input("Contact Email", help='Required', key='txtContactEmail')
        txtRights = st.text_input("Rights", help='Required', value='Public Use', key='txtRights')
    with col2:
        ddlFrequency = st.selectbox("Frequency",key='ddlFrequency', help='Required', options=('Irregular','Continuously updated','Hourly','Daily','Twice a week','Semiweekly','Biweekly','Semimonthly','Monthly','Every Two Months','Quarterly','Semiannual','Biennial','Decennial'))
        txtTags = st.text_input("Tags", help='Required', value='Snowflake', key='txtTags')
        txtOwnerOrg = st.text_input("Owner Org", help='Required',disabled=1,value='sf-testing', key='txtOwnerOrg')
        ddlDatabaseToPublish = st.selectbox("Database", options=getDatabases(), help='Required', key='ddlDatabaseToPublish')
        ddlSchemaToPublish = st.selectbox("Schema", options=getSchemas(), help='Required', key='ddlSchemaToPublish')
    ddlTableToPublish = st.selectbox("Tables to Publish", options=getTables(), help='Required', key='ddlTableToPublish')
    
    col3,col4 = st.columns(2)
    with col3:
         btnPublish = st.button("Publish", on_click=publishTable, type='primary')
    with col4:
        btnRefresh = st.button("Refresh", on_click=refresh)
