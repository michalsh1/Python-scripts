from collections import Counter

import pyodbc
import pandas
import numpy as np

def connect(path):
    try:
        odbc_connection_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % (db_path)
        connection = pyodbc.connect(odbc_connection_str)
        print('connected successfully')
        return connection
    except pyodbc.Error as e:
        print('error in connection', e)

db_path = "C:/Users/michalsh/Downloads/TurtlesDB_be.mdb"
connection = connect(db_path)
# query = "SELECT * FROM ContactPosition"
# cursor = connection.cursor()
# cursor.execute(query)
# cursor.columns()
# cursor.fetchone()
# for row in cursor.fetchall():
#     print(row)

# df= pandas.read_sql(query, connection)
# df.head()


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
# query_ = "SELECT * FROM Turtle"
# query_ = "SELECT * FROM "


df_AcCrawl = pandas.read_sql(query_AcCrawl, connection)
df_AcHatch = pandas.read_sql(query_AcHatch, connection)
df_Activities = pandas.read_sql(query_Activities, connection)
df_Clutches = pandas.read_sql(query_Clutches, connection)
df_ClutchToNest = pandas.read_sql(query_ClutchToNest, connection)
df_Contact = pandas.read_sql(query_Contact, connection)
df_ContactPosition = pandas.read_sql(query_ContactPosition, connection)
df_Crawl = pandas.read_sql(query_Crawl, connection)
df_CrawlContact = pandas.read_sql(query_CrawlContact, connection)
df_CrawlPredator = pandas.read_sql(query_CrawlPredator, connection)
df_Hatcheries = pandas.read_sql(query_Hatcheries, connection)
df_Immerging = pandas.read_sql(query_Immerging, connection)
df_Location = pandas.read_sql(query_Location, connection)
df_Nest = pandas.read_sql(query_Nest, connection)
df_Organization = pandas.read_sql(query_Organization, connection)
df_Specie = pandas.read_sql(query_Specie, connection)
df_TurtleEvent = pandas.read_sql(query_TurtleEvent, connection)
df_Regions = pandas.read_sql(query_Regions, connection)


### contact df:
# df_Contact['FullName']= df_Contact['ContactFname'] + ' ' + df_Contact['ContactLname']
# df_Contact['FullNameTRY']= df_Contact['ContactFname'].astype(str) + ' ' + df_Contact['ContactLname'].astype(str)
df_Contact['FullName']= df_Contact['ContactFname'].fillna('') + ' ' + df_Contact['ContactLname'].fillna('')

df_Contacts_clean = df_Contact.drop(['ContactFname',
                                     'ContactLname',
                                     'ContactPhoto',
                                     'ContactShirt',
                                     'ContactActive',
                                     'SyprusCoures',
                                     'ContactBirthDate',
                                     'ContactStreet',
                                     'ContactHouseNum',
                                     'ContactPostNum',
                                     'ContactCity',
                                     'ContactZipCode',
                                     'ContactCountry',
                                     'ContactSite',
                                     'MailActive',
                                     'MailPreference',
                                     'CotactAddingDate',
                                     'VolunteerGuided',
                                     'VolunteerInstruction',
                                     'VolunteerForms',
                                     'VolunteerInsurance',
                                     'ContactOwner',
                                     'ContactOwnerCode'], axis=1)

df_ContactPosition_Organization = pandas.merge(df_ContactPosition, df_Organization, left_on='Organization', right_on='OrganizationID', how='outer')
df_Contacts_Position_Organization = pandas.merge(df_Contacts_clean, df_ContactPosition_Organization, left_on='ContactPosition', right_on='PositionID', how='outer')


### location df
df_Regions_clean = df_Regions.drop(['RegionSite','RegionNorth','RegionSouth','RegionEast','RegionWest'],axis=1)
df_Location_Regions = pandas.merge(df_Location, df_Regions_clean, left_on= 'Region', right_on='RegionId', how='outer')


### Crawls df
df_Crawl_clean = df_Crawl.drop(['RegionalCrawlID'], axis=1)
df_Specie_clean = df_Specie.drop(['SpeciePic'], axis=1)
df_Crawl_Specie = pandas.merge(df_Crawl_clean, df_Specie_clean, left_on='SpecieID', right_on='SpecieId',)
df_Crawl_Specie_Location = pandas.merge(df_Crawl_Specie, df_Location_Regions, left_on= 'Location', right_on='LocationID')

# df_CrawlContact.shape[0]==df_CrawlContact.CrawlID.nunique()

### check for nan FullNames:
for n in range(0,len(df_Contacts_Position_Organization)):
    print(df_Contacts_Position_Organization['FullName'][n],df_Contacts_Position_Organization['ContactId'][n])

# c = Counter()
# for n in range(0,len(df_CrawlContact)):
#     crawl_id = df_CrawlContact['CrawlID'][n]
#     contactid = df_CrawlContact['ContactID'][n]
#     print('crawl_id: ', crawl_id,'ContactID: ', contactid)
#     c[crawl_id]+=1




## להוסיף לפי Crawl id  את שם המדווח - אם יש כמה שמות- את הסטרינג שלהם, אם לא- אז את סטרינג המקור של שם המדווח

def GetContactStr(crawlcontact_id):
    iloc_contact_id = np.where(df_Contacts_Position_Organization['ContactId'] == crawlcontact_id)
    contact = df_Contacts_Position_Organization.loc[df_Contacts_Position_Organization['ContactId'] == crawlcontact_id]
    # contact = df_Contacts_Position_Organization.iloc[iloc_contact_id[0]]
    contact_name = contact['FullName'].item()
    return contact_name

df_Crawl_Specie_Location_Contacts=df_Crawl_Specie_Location
df_Crawl_Specie_Location_Contacts['observers_names'] = ''



crawl_ids_counter=Counter(df_CrawlContact.CrawlID)
crawl_ids_counter=Counter(df_CrawlContact.CrawlID)

for crawl_id in crawl_ids_counter.keys():
    # ilocs_of_crawl = np.where(df_CrawlContact['CrawlID'] == crawl_id)[0]
    # contacts_ids=df_CrawlContact.iloc[ilocs_of_crawl]['ContactID']
    # print("a",contacts_ids)

    contacts_ids=df_CrawlContact.loc[df_CrawlContact.CrawlID == crawl_id]
    contact_list = []
    for crawlcontact_id in contacts_ids['ContactID']:
        contact_name = GetContactStr(crawlcontact_id)
        contact_list.append(contact_name)
    contacts_str = ', '.join(contact_list)

    iloc_crawl_id = np.where(df_Crawl_Specie_Location_Contacts['CrawlID'] == crawl_id)
    print(crawl_id,iloc_crawl_id, contacts_str)
        # contact = df_Crawl_Specie_Location_Contacts.iloc[iloc_contact_id[0]]






# pandas.merge(df_ClutchToNest,df_Nest,)
# a= pandas.merge(df_Nest)
# b=pandas.merge(a,df_Clutches)


# xlwriter = pandas.ExcelWriter('turtles_df_excel_try3.xlsx')
# df_Location_Regions.to_excel(xlwriter, sheet_name='a', index=False)
# xlwriter.close()






# xlwriter = pandas.ExcelWriter('turtles_df_excel.xlsx')
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