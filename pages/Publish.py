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
    
    valid_email_regex = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""
    compiled_regex = re.compile(valid_email_regex)

    if txtDesc and ddlAccessLevel and txtContactEmail and txtContactName and txtRights and ddlFrequency and txtTags and txtOwnerOrg and ddlTableToPublish and compiled_regex.match(txtContactEmail):
        session = Session.builder.configs(st.session_state.connection_parameters).create()
        #Fully Qualified Table Name
        FQTN = "{0}.{1}.{2}".format(ddlDatabaseToPublish, ddlSchemaToPublish, ddlTableToPublish)
        dfControl = session.create_dataframe([[None,txtDesc,ddlAccessLevel,txtContactName,txtContactEmail,txtRights,ddlFrequency,txtTags,txtOwnerOrg,FQTN]],schema=["Notes","Access_Level","Contact_Name","Contact_Email","Rights","Accural_Periodicity","Tags","Owner_Org"])
        #insert
        dfControl.write.mode("append").save_as_table("{0}.{1}.{2}".format(Control_DB,Control_Schema,Control_Table))
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
            st.error('Not a valid email')
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
        st.session_state.tables = st.session_state.tables = session.sql('SHOW TABLES').collect()
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
    btnPublish = st.button("Publish", on_click=publishTable)