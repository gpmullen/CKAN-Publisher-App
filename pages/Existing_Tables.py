import streamlit as st
from snowflake.snowpark import Session
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import hashlib

if 'connection_parameters' not in st.session_state:
    st.error('Set Context first!')
else:
    session = Session.builder.configs(st.session_state.connection_parameters).create()
    tbls = session.sql('select * from control')
    df = tbls.to_pandas()
    gd = GridOptionsBuilder.from_dataframe(df)
    gd.configure_pagination(enabled=True)
    gd.configure_default_column(editable=True, groupable=True)
    gd.configure_selection(selection_mode="multiple", use_checkbox=True)
    gridoptions = gd.build()
    grid_response= AgGrid(df,gridOptions=gridoptions,update_mode=GridUpdateMode.SELECTION_CHANGED)
    btnDelete = st.button('Delete')
    sel_row = grid_response["selected_rows"]
    if btnDelete:
        for row in sel_row:
            if row["PACKAGE_ID"]:                
                session.sql("DELETE from CONTROL WHERE package_id  = '{0}' ".format(row["PACKAGE_ID"])).collect()
            else:
                concat_cols = row["NOTES"] + row["ACCESSLEVEL"] + row["CONTACT_NAME"] + row["CONTACT_EMAIL"] + row["RIGHTS"] + row["ACCRUALPERIODICITY"] + row["TAG_STRING"] + row["OWNER_ORG"] + row["DATABASE_NAME"] + row["SCHEMA_NAME"] + row["TABLE_NAME"] + row["STATUS"]
                md5 = hashlib.md5(concat_cols.encode()).hexdigest()                
                session.sql("DELETE from CONTROL WHERE md5(concat(NOTES,ACCESSLEVEL,CONTACT_NAME,CONTACT_EMAIL,RIGHTS,ACCRUALPERIODICITY,TAG_STRING,OWNER_ORG,DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,STATUS))  = '{0}' ".format(md5)).collect()        
    session.close()