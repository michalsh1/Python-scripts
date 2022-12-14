from collections import Counter

import pyodbc
import pandas as pd
import numpy as np

def connect(db_path):
    try:
        odbc_connection_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % (db_path)
        connection = pyodbc.connect(odbc_connection_str)
        print('connected successfully')
        return connection
    except pyodbc.Error as e:
        print('error in connection', e)

db_path = "C:/Users/michalsh/Downloads/TurtlesDB_be.mdb"
connection = connect(db_path)

query_AcCrawl = "SELECT * FROM AcCrawl"
query_AcHatch = "SELECT * FROM AcHatch"
query_Activities = "SELECT * FROM Activities"
query_Clutches = "SELECT * FROM Clutches"
query_ClutchToNest = "SELECT * FROM ClutchToNest"
query_Contact = "SELECT * FROM Contact"
query_ContactPosition = "SELECT * FROM ContactPosition"
query_Crawl = "SELECT * FROM Crawl"
query_CrawlContact = "SELECT * FROM CrawlContact"
query_CrawlPredator = "SELECT * FROM CrawlPredator"
query_Hatcheries = "SELECT * FROM Hatcheries"
query_Immerging = "SELECT * FROM Immerging"
query_Location = "SELECT * FROM Location"
query_Nest = "SELECT * FROM Nest"
query_Organization = "SELECT * FROM Organization"
query_Specie = "SELECT * FROM Specie"
query_TurtleEvent = "SELECT * FROM TurtleEvent"
query_Regions = "SELECT * FROM Regions"


df_AcCrawl = pd.read_sql(query_AcCrawl, connection)
df_AcHatch = pd.read_sql(query_AcHatch, connection)
df_Activities = pd.read_sql(query_Activities, connection)
df_Clutches = pd.read_sql(query_Clutches, connection)
df_ClutchToNest = pd.read_sql(query_ClutchToNest, connection)
df_Contact = pd.read_sql(query_Contact, connection)
df_ContactPosition = pd.read_sql(query_ContactPosition, connection)
df_Crawl = pd.read_sql(query_Crawl, connection)
df_CrawlContact = pd.read_sql(query_CrawlContact, connection)
df_CrawlPredator = pd.read_sql(query_CrawlPredator, connection)
df_Hatcheries = pd.read_sql(query_Hatcheries, connection)
df_Immerging = pd.read_sql(query_Immerging, connection)
df_Location = pd.read_sql(query_Location, connection)
df_Nest = pd.read_sql(query_Nest, connection)
df_Organization = pd.read_sql(query_Organization, connection)
df_Specie = pd.read_sql(query_Specie, connection)
df_TurtleEvent = pd.read_sql(query_TurtleEvent, connection)
df_Regions = pd.read_sql(query_Regions, connection)


### contact df:

### --- Generate FullName column without null values.
# df_Contact['FullName']= df_Contact['ContactFname'] + ' ' + df_Contact['ContactLname']
df_Contact['FullName']= df_Contact['ContactFname'].fillna('') + ' ' + df_Contact['ContactLname'].fillna('')

df_Contacts_clean = df_Contact[['ContactId', 'ContactIDNum', 'ContactPosition', 'BeachSurveyor',
       'NestRelocator', 'NestExcavator', 'ContactGender', 'ContactE-mail',
       'ContactNotes','FullName']]

### test- check for nan FullNames:
if df_Contacts_clean['FullName'].isnull().values.any():
    print ('there are null values!')

# for n in range(0,len(df_Contacts_clean)):
#     print(n, df_Contacts_clean['FullName'][n],df_Contacts_clean['ContactId'][n])

df_ContactPosition_Organization = pd.merge(df_ContactPosition, df_Organization, left_on='Organization', right_on='OrganizationID', how='left')
df_Contacts_Position_Organization = pd.merge(df_Contacts_clean, df_ContactPosition_Organization, left_on='ContactPosition', right_on='PositionID', how='left')
# print(df_Contact.shape[0] == df_Contacts_Position_Organization.shape[0])


### location df
df_Regions_clean = df_Regions.drop(['RegionSite', 'RegionNorth', 'RegionSouth', 'RegionEast', 'RegionWest'], axis=1)
df_Location_Regions = pd.merge(df_Location, df_Regions_clean, left_on='Region', right_on='RegionId', how='left')
# print(df_Location.shape[0] == df_Location_Regions.shape[0])


### Crawls df
df_Crawl_clean = df_Crawl.drop(['RegionalCrawlID'], axis=1)
df_Specie_clean = df_Specie.drop(['SpeciePic'], axis=1)
df_Crawl_Specie = pd.merge(df_Crawl_clean, df_Specie_clean, left_on='SpecieID', right_on='SpecieId',how='left')
df_Crawl_Specie_Location = pd.merge(df_Crawl_Specie, df_Location_Regions, left_on= 'Location', right_on='LocationID', how='left')
# print(df_Crawl.shape[0] == df_Crawl_Specie_Location.shape[0])

# df_CrawlContact.shape[0]==df_CrawlContact.CrawlID.nunique()



def GetContact(crawlcontact_id):
    contact = df_Contacts_Position_Organization.loc[df_Contacts_Position_Organization['ContactId'] == crawlcontact_id]
    # contact_name = contact['FullName'].item()
    return contact


df_Crawl_Specie_Location_Contacts = df_Crawl_Specie_Location
# df_Crawl_Specie_Location_Contacts['main_observer_name'] = ''
# df_Crawl_Specie_Location_Contacts['other_observers_names'] = ''


crawl_ids_counter = Counter(df_CrawlContact.CrawlID)

for crawl_id in crawl_ids_counter.keys():
    more_contacts_str =''
    contacts_ids=df_CrawlContact.loc[df_CrawlContact.CrawlID == crawl_id]
    contact_list = []

    for crawlcontact_id in contacts_ids['ContactID']:
        contact = df_Contacts_Position_Organization.loc[df_Contacts_Position_Organization['ContactId'] == crawlcontact_id]
        contact_name = contact['FullName'].item()
        contact_list.append(contact_name)

    main_contact_name = contact_list[0]
    if len(contact_list)>0:
        more_contacts_str = ', '.join(contact_list[1:])

    updating_index = np.where(df_Crawl_Specie_Location_Contacts['CrawlID']==crawl_id)[0][0]
    df_Crawl_Specie_Location_Contacts.at[updating_index,'other_observers_names'] = more_contacts_str
    df_Crawl_Specie_Location_Contacts.at[updating_index,'main_observer_name'] = main_contact_name
    df_Crawl_Specie_Location_Contacts.at[updating_index, 'CrawlID']

    contacts_ids_list = []
    for i in contacts_ids['ContactID']:
        contacts_ids_list.append(str(i))
    contacts_ids_str = ', '.join(contacts_ids_list)
    df_Crawl_Specie_Location_Contacts.at[updating_index, 'contacts_ids'] = contacts_ids_str

    print('new CrawlID: ', df_Crawl_Specie_Location_Contacts.at[updating_index, 'CrawlID'])
    print('new main_observer_name: ',df_Crawl_Specie_Location_Contacts.at[updating_index, 'main_observer_name'])
    print('new other_observers_names: ',df_Crawl_Specie_Location_Contacts.at[updating_index, 'other_observers_names'])
    print('new contacts_ids: ',df_Crawl_Specie_Location_Contacts.at[updating_index, 'contacts_ids'])
    print('-------------------------')
    print('')



##inspect data
df_Crawl_Specie_Location_Contacts.columns ## which columns there are
df_Crawl_Specie_Location_Contacts.Nest.unique() ## what values this col contains












# pd.merge(df_ClutchToNest,df_Nest,)
# a= pd.merge(df_Nest)
# b=pd.merge(a,df_Clutches)


# xlwriter = pd.ExcelWriter('turtles_df_excel_try3.xlsx')
# df_Location_Regions.to_excel(xlwriter, sheet_name='a', index=False)
# xlwriter.close()






# xlwriter = pd.ExcelWriter('turtles_df_excel.xlsx')
# df_AcCrawl.to_excel(xlwriter, sheet_name='AcCrawl', index=False)
# df_AcHatch.to_excel(xlwriter, sheet_name='AcHatch', index=False)
# df_Activities.to_excel(xlwriter, sheet_name='Activities', index=False)
# df_Clutches.to_excel(xlwriter, sheet_name='Clutches', index=False)
# df_ClutchToNest.to_excel(xlwriter, sheet_name='ClutchToNest', index=False)
# df_Contact.to_excel(xlwriter, sheet_name='Contact', index=False)
# df_ContactPosition.to_excel(xlwriter, sheet_name='ContactPosition', index=False)
# df_Crawl.to_excel(xlwriter, sheet_name='Crawl', index=False)
# df_CrawlContact.to_excel(xlwriter, sheet_name='CrawlContact', index=False)
# df_CrawlPredator.to_excel(xlwriter, sheet_name='CrawlPredator', index=False)
# df_Hatcheries.to_excel(xlwriter, sheet_name='Hatcheries', index=False)
# df_Immerging.to_excel(xlwriter, sheet_name='Immerging', index=False)
# df_Location.to_excel(xlwriter, sheet_name='Location', index=False)
# df_Nest.to_excel(xlwriter, sheet_name='Nest', index=False)
# df_Organization.to_excel(xlwriter, sheet_name='Organization', index=False)
# df_Specie.to_excel(xlwriter, sheet_name='Specie', index=False)
# df_TurtleEvent.to_excel(xlwriter, sheet_name='TurtleEvent', index=False)
# xlwriter.close()