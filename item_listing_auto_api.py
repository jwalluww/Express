#************************#
#***** Item Listing *****#
#************************#

def item_listing():
    
    #-------------------------------------------------------------------#
    #--- Purpose:                                                    ---#
    #-------------------------------------------------------------------#
    
    ### NOTES ###
    # Join on SKU
    
    ########################
    ### Import Libraries ###
    ########################
    #%%
    # Main imports
    import pandas as pd
    import numpy as np
    import os
    import pytz
    from datetime import timedelta
    
    # Primary folder for scripts & passcodes
    folder = "G:\\.shortcut-targets-by-id\\1-T64nMCJ6WrtE7nvU7MzJPa6nzDxRw8c\\Justin&Carmen\\Carmen data\\Automation"
    
    # Change the current directory to specified directory
    os.chdir(folder)
    
    # Import Business Functions
    import biz_funcs as bf
    
    # Set Directories & Passcodes to Google
    folder,drive,client,bqclient,credentials,project_id = bf.google_credentials()
    
    # current time where script running
    current_datetime = pd.Timestamp.now(tz=pytz.timezone('US/Central')).replace(tzinfo=None)
    matchtime = current_datetime.to_pydatetime() - timedelta(hours=3)
    matchdate = matchtime.date()
    
    #%%
    
    
    
    #**********************#
    #** Send Slack Alert **#
    #**********************#
    #%%
    bf.slack_start(file_name = 'item_listing')
    #%%
    
    
    
    ###################################
    ### Item Listing ManualSourcing ###
    ###################################
    #%%
    # Read
    ms = bf.read_gsheet(client=client, worksheet='Item Listing', tab='ManualSourcing')
    
    # keep column order
    col_ord = ms.columns.to_list()
    
    # Add New Col
    ms['vendorSKU'] = ms['sku'].str.split('-').str[-1]
    
    #%%
    
    
    
    ############################
    ### Listed Catalog wm-us ###
    ############################
    #%%
    # Read
    listed = bf.read_gsheet(client=client, worksheet='Listed Catalog', tab='wm-us')
    
    # Rename
    listed = listed.rename(columns={'SKU':'sku','vendorURL':'vendorUrl','wmURL':'wmUrlListed'})
    
    # Create New Column
    listed['index'] = listed.index + 1
    listed['index'] = listed['index'].astype(str)
    
    # Keep
    listed = listed[['sku','daysActive','unitsSold','vendorUrl','itemID','index','wmUrlListed']]
    #%%
    
    
    
    ###################################
    ### Walmart US Sourcing - Lowes ###
    ###################################
    #%%
    # Read
    query = ('SELECT * FROM `carmen-8387920.database.lous_dash`')
    lous = bqclient.query(query).to_dataframe().drop_duplicates()
    
    # Rename
    lous = lous.rename(columns={'wmURL':'wmUrl'})
    
    # Keep
    lous = lous[[
        'vendorSKU',
        'wmTitle',
        'vendorTitle',
        'wmBrand',
        'vendorBrand',
        'suggestedPrice',
        'ourMaxPrice',
        'wmImage',
        'vendorImage',
        'vendorUPC',
        'wmUrl',
        ]]
    #%%
    
    
    
    ########################################
    ### Walmart US Sourcing - Home Depot ###
    ########################################
    #%%
    # Read
    query = ('SELECT * FROM `carmen-8387920.database.hdus_wmus_sourcing`')
    hdus = bqclient.query(query).to_dataframe().drop_duplicates()
    
    # Rename
    hdus = hdus.rename(columns={'wmURL':'wmUrl'})
    
    # Keep
    hdus = hdus[[
        'vendorSKU',
        'wmTitle',
        'vendorTitle',
        'vendorItemName',
        'wmBrand',
        'vendorBrand',
        'suggestedPrice',
        'ourMaxPrice',
        'wmImage',
        'vendorImageURL',
        'vendorUPC',
        'wmUrl',
        ]]
    #%%
    
    
    
    ##########################
    ### Join Data Together ###
    ##########################
    #%%
    # Merge Current w/LOUS Sourcing
    df = pd.merge(ms,lous,on='vendorSKU',how='left',indicator=True)
    df = df.rename(columns={'_merge':'lousSourcing'})
    df['lousSourcing'] = df['lousSourcing'].map({'left_only':'Not In LOUS Sourcing','both':'In LOUS Sourcing'})
    df = bf.dup_col(df=df, take='last')
    
    # Merge Current w/HDUS Sourcing
    df = pd.merge(df,hdus,on='vendorSKU',how='left',indicator=True)
    df = df.rename(columns={'_merge':'hdusSourcing'})
    df['hdusSourcing'] = df['hdusSourcing'].map({'left_only':'Not In HDUS Sourcing','both':'In HDUS Sourcing'})
    df = bf.dup_col(df=df, take='last')
    
    # Merge Current w/Listed
    df = pd.merge(df,listed,on='sku',how='left',indicator=True)
    df = df.rename(columns={'_merge':'listedCatalog'})
    df['listedCatalog'] = df['listedCatalog'].map({'left_only':'Not In Listed Script','both':'In Listed Script'})
    df = bf.dup_col(df=df, take='last')
    #%%
    
    
    
    #********************#
    #*** Sales Alert! ***#
    #********************#
    #%%
    # Keeping this here because we turn wmUrl into a hyperlink later on
    df['unitsSold'] = pd.to_numeric(df['unitsSold'].replace("",np.nan).fillna(0)).astype(int)
    data = df.groupby(['Listed by','wmUrlListed','itemID']).agg({'unitsSold':'sum'}).reset_index()
    def namesum(df,name):
        data = df[df['Listed by']==name]
        units = data['unitsSold'].sum()
        top = data.sort_values(by='unitsSold', ascending=False)[['wmUrlListed', 'itemID','unitsSold']].head(3)
        if len(top) > 0:
            url1 = top['wmUrlListed'].values[0]
            item1 = top['itemID'].values[0]
            units1 = top['unitsSold'].values[0]
            link1 = f'<{url1}|{item1}>'
        else:
            url1 = ""
            item1 = ""
            units1 = ""
            link1 = "No Sales This Week"
                
        if len(top) > 1:
            url2 = top['wmUrlListed'].values[1]
            item2 = top['itemID'].values[1]
            units2 = top['unitsSold'].values[1]
            link2 = f'<{url2}|{item2}>'
        else:
            url2 = ""
            item2 = ""
            units2 = ""
            link2 = "No Second SKU This Week"
    
        if len(top) > 2:
            url3 = top['wmUrlListed'].values[2]
            item3 = top['itemID'].values[2]
            units3 = top['unitsSold'].values[2]
            link3 = f'<{url3}|{item3}>'
        else:
            url3 = ""
            item3 = ""
            units3 = ""
            link3 = "No Third SKU This Week"
        
        message_url = f"""Top 3 Selling SKUs
        {link1} --- {units1}
        {link2} --- {units2}
        {link3} --- {units3}
        """
        
        message_name = f"*{name}* \n \n Units Sold: {units} \n \n {message_url}"
        return units,top,message_name
    
    martin_units, martin_skus, martin_message = namesum(df=data,name='Martin')
    dovydas_units, dovydas_skus, dovydas_message = namesum(df=data,name='Dovydas')
    carmen_units, carmen_skus, carmen_message = namesum(df=data,name='Carmen')
       
    message = f"{martin_message} \n \n \n {dovydas_message} \n \n \n {carmen_message}"
    #%%
    
    
    
    ##############################
    ### Calculations & Filters ###
    ##############################
    #%%  
    # Add fixed columns
    df['productIdType'] = 'UPC'
    df['ShippingWeight'] = 1
    df['fulfillmentLagTime'] = 5
    
    # Iterrows through and create hyperlink for gsheets
    df['SellerCenter🔗'] = "https://seller.walmart.com/item/list?filters=%257B%2522productOfferId%2522%253A%2522" + df['sku'] + "%2522%257D"
    
    # Find spot in listed for specific link {col1}:{row1}
    # df['Listed🔗'] = np.where(df['index'].fillna("")!="","=HYPERLINK(" + "https://docs.google.com/spreadsheets/d/17Z-Gj8sK6ahvsrmGS1tNa4Sfn-EdKNz76uqKei_cZvI/edit#gid=1707065011&range=B" + df['index'] + "," + "View in Listed" + ")","")
    # df = df.drop_duplicates(subset=df.columns.difference(['vendorLastScan']), keep='first')
    listed_hyperlink = '"Yes"'
    df['Listed🔗'] = np.where(df['index'].fillna("")!="",'=HYPERLINK("https://docs.google.com/spreadsheets/d/17Z-Gj8sK6ahvsrmGS1tNa4Sfn-EdKNz76uqKei_cZvI/edit#gid=1707065011&range=B' + df['index'] + '",' + listed_hyperlink + ")","")
    
    # Add existing columns
    df = df.replace('',np.nan)
    df['productName'] = df['wmTitle'].combine_first(df['vendorTitle']).combine_first(df['vendorItemName']).combine_first(df['productName'])
    df['brand'] = df['wmBrand'].combine_first(df['vendorBrand'].combine_first(df['brand']))
    df['price'] = df['suggestedPrice'].combine_first(df['ourMaxPrice']).combine_first(df['price'])
    df['shortDescription'] = df['wmTitle'].combine_first(df['vendorTitle']).combine_first(df['vendorItemName']).combine_first(df['shortDescription'])
    df['mainImageUrl'] = df['wmImage'].combine_first(df['vendorImage']).combine_first(df['vendorImageURL']).combine_first(df['mainImageUrl'])
    
    def create_hyperlink(df):
        return f'=HYPERLINK("{df["vendorUrl"]}", "{df["vendorSKU"]}")'
    df['vendorURL'] = np.where((df['vendorUrl'].fillna("")!="")&(df['vendorSKU'].fillna("")!=""),df.apply(create_hyperlink, axis=1),"")
    
    def create_hyperlink(df):
        return f'=HYPERLINK("{df["wmUrl"]}", "{df["itemID"]}")'
    df['wmURL'] = np.where((df['wmUrl'].fillna("")!="")&(df['itemID'].fillna("")!=""),df.apply(create_hyperlink, axis=1),"")
    
    df['vendorUPC'] = np.where(df['vendorUPC'].isna(),"",df['vendorUPC'].astype(str).str.zfill(12))
    df['productId'] = np.where(df['productId'].isna(),"",df['productId'].astype(str).str.zfill(12))
    #%%
    
    
    
    ###################################
    ### Write to Item Listing Sheet ###
    ###################################
    #%%
    # Keep & Order
    df = df[col_ord].fillna("").drop_duplicates()
    
    # Write
    bf.write_gsheet(client=client, df=df, sheet='Item Listing', tab='ManualSourcing',replace=True)
    
    # get hyperlinks to work
    worksheet = client.open('Item Listing').worksheet(('ManualSourcing'))
    cell_range1 = f"A1:A{len(df)+1}"
    cell_range2 = f"P1:Q{len(df)+1}"
    cells1 = worksheet.range(cell_range1)
    cells2 = worksheet.range(cell_range2)
    cells = cells1 + cells2
    worksheet.update_cells(cells,value_input_option='USER_ENTERED')
    #%%
    
    

    ########################
    ### Send Slack Alert ###
    ########################
    #%%
    bf.slack_message(file_name = 'item_listing')

    # Just send at 2AM
    if matchtime.hour in [23]:
        bf.slack_itemlisting(today=matchdate,message=message)
    
    
    
    
if __name__ == "__main__":
    item_listing()