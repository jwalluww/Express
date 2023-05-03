#########################
### Business Data App ###
#########################

import streamlit as st
import pandas as pd
import numpy as np
import gspread as gs
import os

# Primary folder for scripts & passcodes
folder = "G:\\.shortcut-targets-by-id\\1-T64nMCJ6WrtE7nvU7MzJPa6nzDxRw8c\\Justin&Carmen\\Carmen data\\Automation"

# Change the current directory to specified directory
os.chdir(folder)

# Import Business Functions
import biz_funcs as bf

# Set Directories & Passcodes to Google
folder,drive,client,bqclient,credentials,project_id = bf.google_credentials()

# import database name
dbmn = bf.read_gsheet(client=client, worksheet="Listed Catalog", tab='values', twoheader=True)

# Print Table Streamlit
wmus = dbmn[(dbmn['Marketplace']=='WMUS')&(dbmn['Vendor']=='LOUS')][['Auto Pilot']]

st.table(wmus)