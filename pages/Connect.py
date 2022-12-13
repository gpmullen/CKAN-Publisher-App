import streamlit as st
import json
from snowflake.snowpark import Session

try:
    #attempt file based credentials
    with open('creds.json') as f:
        data = json.load(f)
        username = data['username']
        password = data['password']
        account = data["account"]
except:
    username = ''
    password = ''
    account = ''
if 'getContext_set' not in st.session_state:
    st.session_state.getContext_set = False

def getContext(txtUserName, txtAccountLocator,txtPassword):
    if txtUserName and txtAccountLocator and txtPassword:
        if 'connection_parameters' in st.session_state:
            CONNECTION_PARAMETERS = st.session_state.connection_parameters
        else:
            CONNECTION_PARAMETERS = {
            'account': txtAccountLocator,
            'user': txtUserName,
            'password': txtPassword,
            'loginTimeout': 10
            }
        session = Session.builder.configs(CONNECTION_PARAMETERS).create()
        currentwh = session.sql('select current_warehouse()').collect()
        if currentwh[0][0] is None: #need a warehouse to check for roles assigned to the user
            fullroles = session.sql('SHOW ROLES').collect()
            roles = [row[1] for row in fullroles]
            st.info('All roles displayed because a Warehouse was not selected. Set Context with a WH and Get Context again for a filtered list of roles associated to your user.')
        else: #get all the roles in Snowflake
            roles = session.sql('select value::STRING ROLE from table(flatten(input => parse_json(current_available_roles())))').to_pandas()
            
        whs = session.sql('SHOW WAREHOUSES').collect()
        fulldbs = session.sql('SHOW DATABASES').collect()
        dbs = [db[1] for db in fulldbs]
        schemas = session.sql('SHOW SCHEMAS').collect()
        
        st.session_state.roles = roles
        st.session_state.whs = whs
        st.session_state.dbs = dbs
        st.session_state.schemas = schemas
        
        session.close()
        st.session_state.getContext_set = True
    else:
        if not txtUserName:
            st.info('User name is empty')
        if not txtPassword:
            st.info('Password is empty')
        if not txtAccountLocator:
            st.info('Account Locator is empty')

def getListValues(key):
    if key not in st.session_state:
        return []
    else:
        return st.session_state[key]

def filterSchema(ddlDatabase):
    if ddlDatabase:
        lstSchemas= getListValues('schemas')
        return [row[1] for row in lstSchemas if row[4] == ddlDatabase]
    else:
        return []
def setContext(txtAccountLocator,txtUserName,txtPassword,ddlSchema, ddlDatabase,ddlWarehouse,ddlRoles):
    CONNECTION_PARAMETERS = {
    'account': txtAccountLocator,
    'user': txtUserName,
    'password': txtPassword,
    'schema': ddlSchema,
    'database': ddlDatabase,
    'warehouse': ddlWarehouse,
    'role': ddlRoles
    }
    st.session_state.connection_parameters = CONNECTION_PARAMETERS
    st.session_state.context_set = True
    st.success('Saved!', icon="✅")
    
with st.form('GetContext'):
    txtAccountLocator = st.text_input('Account Locator',key='txtAccountLocator', value=account)
    txtUserName = st.text_input('User Name', value=username)
    txtPassword = st.text_input('Password', type="password", value=password)
    btnConnect = st.form_submit_button('Get Context', on_click=getContext,args=[txtUserName, txtAccountLocator,txtPassword],type='secondary')

if st.session_state.getContext_set:
    with st.container():
        ddlRoles = st.selectbox('Roles', options=getListValues('roles'))
        ddlWarehouse = st.selectbox('Warehouse', options=getListValues('whs'))
        ddlDatabase = st.selectbox('Database', options=getListValues('dbs'))
        ddlSchema = st.selectbox('Schema', options=filterSchema(ddlDatabase))
        btnSet = st.button('Set Context', on_click=setContext, args=[txtAccountLocator,txtUserName,txtPassword,ddlSchema, ddlDatabase,ddlWarehouse,ddlRoles], type='primary')

