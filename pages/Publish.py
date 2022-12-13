import streamlit as st
from snowflake.snowpark import Session
import re

def getTables():
    session = Session.builder.configs(st.session_state.connection_parameters).create()
    ret = session.sql('SHOW TABLES').collect()
    session.close()
    return [tbl[1] for tbl in ret]

def publishTable(txtDesc,ddlAccessLevel,txtContactName,txtContactEmail,txtRights,ddlFrequency,txtTags,txtOwnerOrg,ddlTableToPublish):
    valid_email_regex = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""
    compiled_regex = re.compile(valid_email_regex)

    if txtDesc and ddlAccessLevel and txtContactEmail and txtContactName and txtRights and ddlFrequency and txtTags and txtOwnerOrg and ddlTableToPublish and compiled_regex.match(txtContactEmail):
        session = Session.builder.configs(st.session_state.connection_parameters).create()

        dfControl = session.create_dataframe([[txtDesc,ddlAccessLevel,txtContactName,txtContactEmail,txtRights,ddlFrequency,txtTags,txtOwnerOrg]],schema=["Notes","Access_Level","Contact_Name","Contact_Email","Rights","Accural_Periodicity","Tags","Owner_Org"])
        #insert
        dfControl.write.mode("append").save_as_table("Control")
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
    with st.form('frmPublish'):
        st.info('Choose a table to publish. All metadata must be populated.')
        txtDesc = st.text_input("Description", help='Required')
        ddlAccessLevel = st.selectbox("Access Level",options=('Public','Restricted','Non-public'), help='Required')
        txtContactName = st.text_input("Contact Name", help='Required')
        txtContactEmail = st.text_input("Contact Email", help='Required')
        txtRights = st.text_input("Rights", help='Required', value='Public Use')
        ddlFrequency = st.selectbox("Frequency", help='Required', options=('Irregular','Continuously updated','Hourly','Daily','Twice a week','Semiweekly','Biweekly','Semimonthly','Monthly','Every Two Months','Quarterly','Semiannual','Biennial','Decennial'))
        txtTags = st.text_input("Tags", help='Required', value='Snowflake')
        txtOwnerOrg = st.text_input("Owner Org", help='Required',disabled=1,value='sf-testing')
        ddlTableToPublish = st.selectbox("Tables to Publish", options=getTables(), help='Required')
        btnPublish = st.form_submit_button("Publish", on_click=publishTable, args=[txtDesc,ddlAccessLevel,txtContactName,txtContactEmail,txtRights,ddlFrequency,txtTags,txtOwnerOrg,ddlTableToPublish])