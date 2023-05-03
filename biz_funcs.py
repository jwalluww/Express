#************************************#
#***** Business Function Script *****#
#************************************#
#-- Place all Functions here for scripts
#-- Not Automated but called in each script
#-- Includes any reusable code across scripts

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import numpy as np
import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from slack_sdk import WebClient
import datetime



def google_credentials():
    # Primary folder for scripts & passcodes
    folder = "G:\\.shortcut-targets-by-id\\1-T64nMCJ6WrtE7nvU7MzJPa6nzDxRw8c\\Justin&Carmen\\Carmen data\\Automation"
    
    # Change the current directory to specified directory
    os.chdir(folder)
    
    # Connect to Google Sheets
    scope = ['https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive"]
    
    # Credential created through gsheet api on cloud project
    credentials = ServiceAccountCredentials.from_json_keyfile_name("client_secrets_gsheet.json", scope)
    client = gspread.authorize(credentials)
    
    ###
    
    # For CSV readin'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="client_secrets_gsheet.json"
    
    gauth = GoogleAuth()

    # Use service account credentials
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secrets_gsheet.json', scope)
    
    # Set Drive
    drive = GoogleDrive(gauth)
    
    # Set Bigquery client
    bqclient = bigquery.Client() 
    
    # Set Credentials
    credentials = service_account.Credentials.from_service_account_file('client_secrets_gsheet.json')
    
    # Set project ID
    project_id='carmen-8387920'

    return folder,drive,client,bqclient,credentials,project_id




def wm_item_report(drive,folder,country):
    ### Import Latest Item Report CSV ###
    if country == 'us':
        report = "ItemReport_1000000"
        output = 'wmus_item.csv'
        listed = pd.DataFrame(drive.ListFile({'q': "title contains 'ItemReport'"}).GetList())
    elif country == 'ca':
        report = "item_ca"
        output = 'wmca_item.csv'
        listed = pd.DataFrame(drive.ListFile({'q': "title contains 'item'"}).GetList())

    # List out all item reports in g drive

    # Find US item report & download 
    latest_item_report_id = listed[listed['title'].str.startswith(report)].sort_values(by=['createdDate'],ascending=False)['id'].reset_index().drop(columns='index').iloc[0][0]
    file_id = latest_item_report_id # Set desired File ID
    file = drive.CreateFile({'id': file_id}) # Create object for this particular file id
    
    # https://stackoverflow.com/questions/46155300/pydrive-error-no-downloadlink-exportlinks-for-mimetype-found-in-metadata
    # mimetypes = {
    #         # Drive Document files as PDF
    #         'application/vnd.google-apps.document': 'application/pdf',

    #         # Drive Sheets files as MS Excel files.
    #         'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    #         }
    # download_mimetype = None
    # if file['mimeType'] in mimetypes:
    #     download_mimetype = mimetypes[file['mimeType']]
    #     file.GetContentFile(file['title'], mimetype=download_mimetype)

    #     file.GetContentFile(output, mimetype=download_mimetype)
    # else: 
    #     file.GetContentFile(output) # Download file using this command and name whatever is in the blank
    
    file.GetContentFile(output) 

    # Read wmus item report from set directory
    wm_item = pd.read_csv(folder + "\\" + output,dtype={'UPC':'str'}) # no duplicate column names exist
    
    return wm_item



def gdrive_csv(file_name,drive,folder,sep=",",file_ext=".csv"):
    # List out all item reports in g drive
    listed = pd.DataFrame(drive.ListFile({'q': "title contains '"+file_name+"'"}).GetList())

    # Find US item report & download 
    latest_report_id = listed[listed['title'].str.startswith(file_name)].sort_values(by=['createdDate'],ascending=False)['id'].reset_index().drop(columns='index').iloc[0][0]
    file_id = latest_report_id # Set desired File ID
    file = drive.CreateFile({'id': file_id}) # Create object for this particular file id
    file.GetContentFile(file_name+file_ext) # Download file using this command and name whatever is in the blank

    # Read wmus item report from set directory
    if file_ext == ".csv":
        report = pd.read_csv(folder + "\\" + file_name+file_ext,dtype={'UPC':'str','upc':'str','Amazon UPC/EAN':'str'},encoding= 'unicode_escape') # no duplicate column names exist
    else:
        report = pd.read_excel(folder + "\\" + file_name+file_ext,dtype={'UPC':'str','upc':'str'})
    
    return report



def read_gsheet(client,worksheet,tab,twoheader=False):
    sheet = client.open(worksheet).worksheet((tab))
    output = sheet.get_all_values()
    if twoheader == True:
        output.pop(0)
    headers = output.pop(0)
    output = pd.DataFrame(output,columns=headers)
    return output



def write_gsheet(client,df,sheet,tab,clear=True):
    sheet = client.open(sheet).worksheet((tab)) # Open up sheet for listing
    if clear==True:
        sheet.clear() # Completely clear sheet
    sheet.update([df.columns.values.tolist()] + df.values.tolist()) # write all of df to sheet 
    


def write_csv(drive,path):
    file=drive.CreateFile()
    file.SetContentFile(path)
    file.Upload()
    
    
    
def fix_format(df,round2,round0,roundpct):
    for i in round2:
        df[i] = np.round(pd.to_numeric(df[i]),2)
    for i in round0:
        df[i] = np.round(pd.to_numeric(df[i]),0)
    for i in roundpct:
        df[i] = np.round(pd.to_numeric(df[i])*100,2)



def slack_message(file_name):
    file_name = file_name
    current_time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    completion = file_name + " completed at " + current_time
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='script-completion-report',text=completion)
    
def slack_error(file_name,e):
    file_name = file_name
    current_time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    completion = file_name + " failed at " + current_time + " because of " + str(e)
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='script-error-report',text=completion)
    
def slack_blacklist(file_name,sku,reason):
    file_name = file_name
    current_time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    completion = file_name+" sku "+sku+" is "+reason+", remove from Whitelist at "+current_time
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='blacklist',text=completion)
    
def slack_blacklist_brands(file_name,sku,reason):
    file_name = file_name
    current_time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    completion = f"Please check if brand needs to be added to blacklist for {sku} in {file_name} as it was flagged as {reason} at {current_time}"
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='blacklist',text=completion)
    
def slack_dailyscans(script,hour,status,marketplace):
    completion = f"{script} Last Scan was {hour} hours ago, Inventory Turned {status} for {marketplace}"
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='dailyscans',text=completion)
    
def slack_skulist(vendor,scan,skulist,both):
    completion = f"{vendor} Daily Scan & SKU List are not properly aligned. There are {scan} SKUs only in the {vendor} Daily Scan, {skulist} SKUs only in the New SKU List, and {both} SKUs in both the scan and the list"
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='script-error-report',text=completion)
    
def slack_shipalerts(file_name,sku_list):
    completion = f"SKUs NotFound/Discontinued from {file_name}: {sku_list}"
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='itemstodelete',text=completion)
    
def slack_salesalerts(message1,message2):
    completion = f"{message1} \n \n \n {message2}"
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='sales',text=completion)
    
def slack_listed_lowes():
    completion = "There is a discrepancy between the Lowes US Daily Scan & the Listed Catalog Lowes SKUs"
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='inventory',text=completion)
    
def slack_ship_alerts(sku,wmBrand,Offer1Seller,reviewsCount,itemID,wmURL,sheet):
    completion = f"""
    These SKUs have shipping fees for {sheet}
    <{wmURL}|{sku}> {wmBrand} {Offer1Seller} {reviewsCount} {itemID}
    """
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='carmen-alerts',text=completion)
    
def slack_shipstate_counts(sku,wmurl,shipstate_counts,sheet):
    completion = f"""
    This SKU has {shipstate_counts} for {sheet}
    <{wmurl}|{sku}>
    """
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='carmen-alerts',text=completion)
    
def slack_order_notes(url,link,mrktOrderId,sku,orderNotes):
    completion = f"""
    <{url}|{mrktOrderId}>
    {orderNotes}
    <{link}|{sku}>
    """
    client = WebClient(token='xoxb-1406191412647-4439119406976-WdPTVjf2409yMNDMl40ABAua')
    client.chat_postMessage(channel='orders',text=completion)    
    
    
    
def dup_col(df,take):
    for i in df.columns:
        if i.endswith('_x'):
            j = i.replace('_x','')
            k = j + '_y'
            if take=='first':
                df[j] = df[i].combine_first(df[k])
            elif take=='last':
                df[j] = df[k].combine_first(df[i])
            elif take=='force first':
                df[j] = df[i]
            elif take=='force last':
                df[j] = df[k]
            df.drop(columns=[i,k],inplace=True)
    return df



def strp(df,col,typ):
    if typ=='dollar':
        df[col] = df[col].str.strip("$").astype(float)
    else:
        df[col] = df[col].str.strip("%").astype(float)/100
    return df
    
    
    
######################
##### EXTRA CODE #####
######################

# BATCH CLEARING GOOGLE SHEET #
# logs2 = pd.merge(logs,df_upc,on='UPC/GTIN',how='outer').astype(str).replace("nan","")
# sheet.batch_clear(["A2:M10000"])