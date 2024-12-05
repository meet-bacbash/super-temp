# Import the domomagic package into the script
import numpy as np
import pandas as pd
import datetime
import re
import requests
import logex
from functools import lru_cache

spark = 0
wrtie_db = 0809432
def add_data(a,b):
    pass

@lru_cache()
def read_pp_orders(key, data_set="pp_orders", db="0b97feb6f01d4bf294e50ddc3a2c2c94"):
    if key == "cox":
        return spark.sql(f"SELECT *,CONVERT(varchar(12), orderdatetime, 108) ordertimePST FROM `{db}`.{data_set} WHERE providerkey = '23378765-B3CE-4A9A-B1A2-F61740D7FE5E'").toPandas()
    elif key == "tmobile":
        return spark.sql(f"SELECT *,CONVERT(varchar(12), orderdatetime, 108) ordertimePST FROM `{db}`.{data_set} WHERE providerkey = '1F68090E-3B3A-41BE-A977-7C84B5A47741'").toPandas()
    elif key == "ws":
        return spark.sql(f"SELECT *,CONVERT(varchar(12), orderdatetime, 108) ordertimePST FROM `{db}`.{data_set} WHERE providerkey = 'AED192CD-277D-4190-8CC9-55F166C05C26'").toPandas()

def wrtie_df(df_to_write, dataset_name, elements):
    df_to_write.write.saveAsTable(f"`{wrtie_db}`.{dataset_name}", mode="overwrite")
    flag = add_data(dataset_name, elements)
    if flag:
        response = requests.get(
            f"http://dataspace-backend:3000/pub/profile-job?dataset={dataset_name}&database={wrtie_db}")
        print(f"http://dataspace-backend:3000/pub/profile-job?dataset={dataset_name}&database={wrtie_db}")
        result = response.json()
        print("result from dataspace profiler =>", result)


@lru_cache()
def read_dataframe(dataset_name, read_db="6326304480444d9683aa4767373e10b9"):
    return spark.sql(f"SELECT * FROM `{read_db}`.{dataset_name}").toPandas()



# read data from inputs into a data frame
SPC = read_dataframe(spark,'spectrum')
SPC = SPC[SPC['retailername'].isin(["TPP Call Center X", 'XYZies - Call Center EX'])]

ATT = read_dataframe(spark,'att')
ATT = ATT[ATT['retailername'].isin(["TPP Call Center X", 'XYZies - Call Center EX'])]

OPT_SUD = read_dataframe(spark,'optimum')

Viasat = read_dataframe(spark,'viasat')
Viasat = Viasat[Viasat['salesrepname'] != 'Administrator Bundle ATT']

Frontier = read_dataframe(spark,'frontier')
Vivint = read_dataframe(spark,'vivint')

Alder = read_dataframe(spark,'alder')

COMCAST = read_dataframe(spark,'comcast')

EARTHLINK = read_dataframe(spark,'earthlink')

DTV = read_dataframe(spark,'dtv')
DTV = DTV[DTV['retailername'].isin(["TPP Call Center X", 'XYZies - Call Center EX'])]

CONSOLIDATE = read_dataframe(spark,'consolidate')

CLEARCONNECT = read_dataframe(spark,'clearconnect')

ADT = read_dataframe(spark,'adt')

COX = read_pp_orders(key='cox')

TMOBILE = read_pp_orders(key='tmobile')

WS = read_pp_orders(key='ws')

# replace sale_made = 1 with sale_made = true
WOW = read_dataframe(spark,'wow')
del WOW['sale_made']
WOW['sale_made'] = np.where(WOW['soldUnits'] > 0, 'true', 'false')

SPC['Provider-Sales'] = 'SPC'
ATT['Provider-Sales'] = 'ATT'
OPT_SUD['Provider-Sales'] = 'OPM'
Viasat['Provider-Sales'] = 'VIASAT'
Frontier['Provider-Sales'] = 'FRONTIER'
Vivint['Provider-Sales'] = 'VIVINT'
Alder['Provider-Sales'] = 'ALDER'
COMCAST['Provider-Sales'] = 'COMCAST'
EARTHLINK['Provider-Sales'] = 'EARTHLINK'
DTV['Provider-Sales'] = 'DTV'
CONSOLIDATE['Provider-Sales'] = 'CONSOLIDATE'
CLEARCONNECT['Provider-Sales'] = 'CLEARCONNECT'
ADT['Provider-Sales'] = 'ADT'
WOW['Provider-Sales'] = 'WOW'
COX['Provider-Sales'] = 'COX'
TMOBILE['Provider-Sales'] = 'TMOBILE'
WS['Provider-Sales'] = 'WS'

all_tracker = read_dataframe(spark,'campaign_tracker')

all_tracker.loc[all_tracker['Provider'] == 'ALL', ['Provider']] = 'SPC'
all_tracker = all_tracker[
    (all_tracker['Postfix'] == 18080) | (all_tracker['Postfix'] == 18081) | (all_tracker['Postfix'] == 18082) | (
                all_tracker['Postfix'] == 99091) | (all_tracker['Postfix'] == 99090) | (
                all_tracker['Postfix'] == 99081) | (all_tracker['Postfix'] == 20082) | (
                all_tracker['Postfix'] == 20081) | (all_tracker['Postfix'] == 20080) | (
                all_tracker['Postfix'] == 50021) | (all_tracker['Postfix'] == 50020) | (
                all_tracker['Postfix'] == 10020) |
    (all_tracker['Postfix'] == 50151) |
    (all_tracker['Postfix'] == 99260) | (all_tracker['Postfix'] == 50100) | (all_tracker['Postfix'] == 20151) | (
                all_tracker['Postfix'] == 99150) | (all_tracker['Postfix'] == 10080) | (
                all_tracker['Postfix'] == 10090) | (all_tracker['Postfix'] == 20082) | (
                all_tracker['Postfix'] == 99080) | (all_tracker['Postfix'] == 99092) | (
                all_tracker['Postfix'] == 18150) | (all_tracker['Postfix'] == 99082) | (
                all_tracker['Postfix'] == 10110) | (
        all_tracker['First Responders'].str.contains("DoppCall 99150", na=False)) | (
                (all_tracker['First Responders'].str.contains("P50", na=False)) & ~(
            all_tracker['First Responders'].str.contains("test", na=False, case=False)))]

# write your script here
Frontier = Frontier.rename(columns={"soldUnits": "bundleUnit"})
Vivint = Vivint.rename(columns={"soldUnits": "bundleUnit"})
COMCAST = COMCAST.rename(columns={"soldUnits": "bundleUnit"})
EARTHLINK = EARTHLINK.rename(columns={"soldUnits": "bundleUnit"})
CONSOLIDATE = CONSOLIDATE.rename(columns={"soldUnits": "bundleUnit"})
CLEARCONNECT = CLEARCONNECT.rename(columns={"soldUnits": "bundleUnit"})
ADT = ADT.rename(columns={"soldUnits": "bundleUnit"})

SPC = SPC.rename(
    columns={"CustomerPhone": "customerPhone", "OrderId": "orderid", "OrderDate": "orderDate", "OrderTime": "orderTime",
             "CampaignName": "campaignName", "SalesRepId": "salesRepId", "SalesRepName": "salesRepName",
             "BundleUnits": "bundleUnit", "RetailerName": "retailerName", "TotalUnits": "totalUnits",
             'CoreUnits': 'SPCcoreUnits', "CustomerFirstName": "customerFirstName",
             "CustomerLastName": "customerLastName", 'Timeframe': 'TimeFrame'})
SPC = SPC.rename(columns={"OrderDate": "orderDate", "OrderTime": "orderTime", "CampaignName": "campaignName",
                          "SalesRepId": "salesRepId", "SalesRepName": "salesRepName", "BundleUnits": "bundleUnit",
                          "RetailerName": "retailerName", "TotalUnits": "totalUnits", "OrderKey": "orderKey",
                          "OrderId": "orderId", "CustomerPhone": "customerPhone"})
Viasat = Viasat.rename(columns={"orderdate": "orderDate", 'ordertime': "orderTime"})
Frontier = Frontier.rename(columns={'customerPhone': 'trash', 'customerCallerId': 'customerPhone'})
OPT_SUD = OPT_SUD.rename(columns={'orderdate': 'orderDate', 'ordertime': 'orderTime'})
WOW = WOW.rename(columns={'OrderDate': 'orderDate', 'OrderTime': 'orderTime', 'RetailerName': 'retailerName',
                          'CampaignName': 'campaignName'})

all_tracker["UID"] = all_tracker["UID"].astype(str)
all_tracker["UID"] = all_tracker["UID"].str.replace("\.0", "")
all_tracker["Parent UID"] = all_tracker["Parent UID"].astype(str)
all_tracker["Parent UID"] = all_tracker["Parent UID"].str.replace("\.0", "")
all_tracker['google space removed'] = all_tracker['Google Campaign Name'].apply(
    lambda x: str(x).replace(' ', '').replace(',', '').replace('-', '').replace(':', '').replace('(', '').replace(')',
                                                                                                                  '').lower())
# all_tracker['portal space removed']= all_tracker['First Responders'].apply(correct_names)
all_tracker['portal space removed'] = all_tracker['First Responders'].apply(
    lambda x: str(x).replace(' ', '').replace(',', '').replace('-', '').replace(':', '').replace('(', '').replace(')',
                                                                                                                  '').lower())
# all_tracker['vonage space removed']= all_tracker['Vonage Campaign Name'].apply(correct_names)
all_tracker['vonage space removed'] = all_tracker['Vonage Campaign Name'].apply(
    lambda x: str(x).replace(' ', '').replace(',', '').replace('-', '').replace(':', '').replace('(', '').replace(')',
                                                                                                                  '').lower())

relation4 = pd.DataFrame()
current_tracker = all_tracker[all_tracker["Current"] == "NEW"]
relation4["Portal Camp"] = current_tracker['First Responders']
# relation1["vonage space removed"] = all_tracker['vonage space removed']
relation4["UID"] = current_tracker["UID"]
relation4 = relation4.drop_duplicates()

all_tracker.loc[all_tracker['EXT'] == 1, ['UID']] = all_tracker['Parent UID']

relation2 = pd.DataFrame()
relation2["portal space removed"] = all_tracker['portal space removed']
relation2["Provider"] = all_tracker['Provider']
relation2["Responsible"] = all_tracker['Responsible']
relation2["UID"] = all_tracker["UID"]

# Revenue Table
proration = read_dataframe(spark,'SPC Proration Rate')

proration = proration[
    ['start date', 'end date', 'Provider-Sales', 'SalesIdDivisionTier', 'Internet proration', 'Video proration',
     'Phone proration']]

Rev_table = read_dataframe(spark,'DOMO Revenue Table with Division')
att_pip = Rev_table[Rev_table['Tier'] == 'PIP'].packageName.unique()

Internet_table = Rev_table[Rev_table['Product'] == 'Internet']
Internet_table = Internet_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

Addon_table = Rev_table[Rev_table['Product'] == 'Addon']
Addon_table = Addon_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

Mobile_table = Rev_table[Rev_table['Product'] == 'Mobile']
Mobile_table = Mobile_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

Phone_table = Rev_table[Rev_table['Product'] == 'Phone']
Phone_table = Phone_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

Security_table = Rev_table[Rev_table['Product'] == 'Security']
Security_table = Security_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

Video_table = Rev_table[Rev_table['Product'] == 'Video']
Video_table = Video_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

Voice_table = Rev_table[Rev_table['Product'] == 'Voice']
Voice_table = Voice_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

Wireless_table = Rev_table[Rev_table['Product'] == 'Wireless']
Wireless_table = Wireless_table[
    ['start date', 'end date', 'Provider-Sales', 'packageName', 'SalesIdDivisionTier', 'Commission from Provider']]

# ATT
## ATT Internet
ATT = pd.merge(ATT, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
               right_on=['Provider-Sales', 'packageName'], how='left')
ATT = ATT[
    (ATT['orderDate'] >= ATT['start date']) & (ATT['orderDate'] <= ATT['end date']) | (ATT['start date'].isnull())]
ATT = ATT.rename(columns={'Commission from Provider': 'internetValue'})
ATT['internetValue'] = ATT['internetValue'].fillna(0)
del ATT['start date']
del ATT['end date']

## ATT Video
ATT = pd.merge(ATT, Video_table, left_on=['Provider-Sales', 'video_credit_risk'],
               right_on=['Provider-Sales', 'packageName'], how='left')
ATT = ATT[
    (ATT['orderDate'] >= ATT['start date']) & (ATT['orderDate'] <= ATT['end date']) | (ATT['start date'].isnull())]
ATT = ATT.rename(columns={'Commission from Provider': 'videoValue'})
ATT['videoValue'] = ATT['videoValue'].fillna(0)
del ATT['start date']
del ATT['end date']

### Extra 50 on Choice, Premier, Ultimate
ATT['videoValue_extra1'] = 0
ATT.loc[ATT['videoPackage'].str.contains('Choice|Premier|Ultimate', na=False, case=False), ['videoValue_extra1']] = 50
ATT.loc[ATT['orderDate'] >= '2023-03-13', ['videoValue_extra1']] = 0

### Extra 160 on PIP + DTV
ATT['videoValue_extra2'] = 0
ATT.loc[
    (ATT['internetPackage'].isin(att_pip)) & (ATT['videoPackage'].str.contains('DTV|Directv', na=False, case=False)), [
        'videoValue_extra2']] = 160
ATT.loc[ATT['orderDate'] >= '2023-03-13', ['videoValue_extra2']] = 0

### Extra 35 on internet>=100mbps and video = Stream
ATT['videoValue_extra3'] = 0
ATT.loc[(ATT['internetValue'] >= 175) & (ATT['videoPackage'].str.contains('stream', na=False, case=False)), [
    'videoValue_extra3']] = 35
ATT.loc[ATT['orderDate'] >= '2023-03-13', ['videoValue_extra3']] = 0

#### Get the total video Value
ATT['videoValue'] = ATT['videoValue'] + ATT['videoValue_extra1'] + ATT['videoValue_extra2'] + ATT['videoValue_extra3']

### ATT NEW Commison
ATT.loc[(ATT['orderDate'] >= '2023-03-13') & (ATT['video_credit_risk'] == 'Low'), ['videoValue']] = ATT[
                                                                                                        'videoValue'] + 40
ATT.loc[(ATT['orderDate'] >= '2023-04-01') & (ATT['orderDate'] <= '2023-04-30') & (ATT['video_credit_risk'] == 'Low'), [
    'videoValue']] = ATT['videoValue'] + 75

## ATT Voice
ATT['voiceValue'] = np.where(~ATT['voicePackage'].isnull(), 70, 0)
ATT.loc[(~ATT['voicePackage'].isnull()) & (ATT['orderDate'] >= '2023-03-13'), ['voiceValue']] = 10

## ATT Wireless
ATT['wirelessValue'] = 175
ATT['wirelessValue'] = ATT['wirelessValue'] * ATT['wirelessLines']
# If it is BYOD, then no commission for wireless
ATT.loc[ATT['wireless_byodLines'] == 1, ['wirelessValue']] = 125 * ATT['wirelessLines']

# Addon Skip for now 2022/07/21
ATT['addonValue'] = 0.0
ATT['Total Value'] = ATT['internetValue'] + ATT['videoValue'] + ATT['voiceValue'] + ATT['addonValue'] + ATT[
    'wirelessValue']
ATT['Month'] = pd.to_datetime(ATT['orderDate']).dt.month
ATT['Year'] = pd.to_datetime(ATT['orderDate']).dt.year

# Rename Revenue from sale for each product
ATT['ATT internetValue'] = ATT['internetValue']
ATT['ATT videoValue'] = ATT['videoValue']
ATT['ATT voiceValue'] = ATT['voiceValue']
ATT['ATT addonValue'] = ATT['addonValue']
ATT['ATT wirelessValue'] = ATT['wirelessValue']

## Combine Install date and status
ATT.loc[(~(ATT['internetInstallDate'].isnull()) & (ATT['internetInstallDate'] != None) & (
            ATT['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Active_updated'
ATT.loc[(~(ATT['videoInstallDate'].isnull()) & (ATT['videoInstallDate'] != None) & (
            ATT['videoOrderLineStatus'] != 'Disconnected')), ['videoOrderLineStatus']] = 'Active_updated'
ATT.loc[(~(ATT['voiceInstallDate'].isnull()) & (ATT['voiceInstallDate'] != None) & (
            ATT['voiceOrderLineStatus'] != 'Disconnected')), ['voiceOrderLineStatus']] = 'Active_updated'
ATT.loc[(~(ATT['wirelessInstallDate'].isnull()) & (ATT['wirelessInstallDate'] != None) & (
            ATT['wirelessOrderLineStatus'] != 'Disconnected')), ['wirelessOrderLineStatus']] = 'Active_updated'

## Combine Disconnect date and status
ATT.loc[(~(ATT['internetDisconnectDate'].isnull()) & (ATT['internetDisconnectDate'] != None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'
ATT.loc[(~(ATT['videoDisconnectDate'].isnull()) & (ATT['videoDisconnectDate'] != None)), [
    'videoOrderLineStatus']] = 'Disconnected_updated'
ATT.loc[(~(ATT['voiceDisconnectDate'].isnull()) & (ATT['voiceDisconnectDate'] != None)), [
    'voiceOrderLineStatus']] = 'Disconnected_updated'
ATT.loc[(~(ATT['wirelessDisconnectDate'].isnull()) & (ATT['wirelessDisconnectDate'] != None)), [
    'wirelessOrderLineStatus']] = 'Disconnected_updated'

# ATT Chargeback
ATT['ATT Chargeback'] = 0
ATT.loc[
    (ATT['internetOrderLineStatus'] == 'Disconnected_updated') | (ATT['internetOrderLineStatus'] == 'Disconnected'), [
        'ATT Chargeback']] = ATT['ATT Chargeback'] + ATT['internetValue']
ATT.loc[(ATT['videoOrderLineStatus'] == 'Disconnected_updated') | (ATT['videoOrderLineStatus'] == 'Disconnected'), [
    'ATT Chargeback']] = ATT['ATT Chargeback'] + ATT['videoValue']
ATT.loc[(ATT['voiceOrderLineStatus'] == 'Disconnected_updated') | (ATT['voiceOrderLineStatus'] == 'Disconnected'), [
    'ATT Chargeback']] = ATT['ATT Chargeback'] + ATT['voiceValue']
ATT.loc[
    (ATT['wirelessOrderLineStatus'] == 'Disconnected_updated') | (ATT['wirelessOrderLineStatus'] == 'Disconnected'), [
        'ATT Chargeback']] = ATT['ATT Chargeback'] + ATT['wirelessValue']

# ATT Disconnect Unit
ATT['ATT Disconnect Unit'] = 0
ATT.loc[
    (ATT['internetOrderLineStatus'] == 'Disconnected_updated') | (ATT['internetOrderLineStatus'] == 'Disconnected'), [
        'ATT Disconnect Unit']] = ATT['ATT Disconnect Unit'] + 1
ATT.loc[(ATT['videoOrderLineStatus'] == 'Disconnected_updated') | (ATT['videoOrderLineStatus'] == 'Disconnected'), [
    'ATT Disconnect Unit']] = ATT['ATT Disconnect Unit'] + 1
ATT.loc[(ATT['voiceOrderLineStatus'] == 'Disconnected_updated') | (ATT['voiceOrderLineStatus'] == 'Disconnected'), [
    'ATT Disconnect Unit']] = ATT['ATT Disconnect Unit'] + 1
ATT.loc[
    (ATT['wirelessOrderLineStatus'] == 'Disconnected_updated') | (ATT['wirelessOrderLineStatus'] == 'Disconnected'), [
        'ATT Disconnect Unit']] = ATT['ATT Disconnect Unit'] + ATT['wirelessLines']

### ATT Revenue from install
ATT['ATT Revenue From Install'] = 0
ATT.loc[(ATT['internetOrderLineStatus'] == 'Active_updated') | (
    ATT['internetOrderLineStatus'].isin(['Active', 'Installed'])), ['ATT Revenue From Install']] = ATT[
                                                                                                       'ATT Revenue From Install'] + \
                                                                                                   ATT['internetValue']
ATT.loc[
    (ATT['videoOrderLineStatus'] == 'Active_updated') | (ATT['videoOrderLineStatus'].isin(['Active', 'Installed'])), [
        'ATT Revenue From Install']] = ATT['ATT Revenue From Install'] + ATT['videoValue']
ATT.loc[
    (ATT['voiceOrderLineStatus'] == 'Active_updated') | (ATT['voiceOrderLineStatus'].isin(['Active', 'Installed'])), [
        'ATT Revenue From Install']] = ATT['ATT Revenue From Install'] + ATT['voiceValue']
ATT.loc[(ATT['wirelessOrderLineStatus'] == 'Active_updated') | (
    ATT['wirelessOrderLineStatus'].isin(['Active', 'Installed'])), ['ATT Revenue From Install']] = ATT[
                                                                                                       'ATT Revenue From Install'] + \
                                                                                                   ATT['wirelessValue']
# Remove Spiff if canncel or disconnect
ATT.loc[(ATT['internetValue'] == 0), ['ATT Revenue From Install']] = ATT['ATT Revenue From Install'] - ATT[
    'videoValue_extra2'] - ATT['videoValue_extra3']

## Comment for now, need to add back after validation
############ ATT Low risk Video Spiff
ATT['ATT Revenue From Install'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 4) & (ATT['video_credit_risk'] == 'Low') & (
                ATT['ATT Revenue From Install'] > 0), ATT['ATT Revenue From Install'] + 25,
    ATT['ATT Revenue From Install'])
ATT['ATT Revenue From Install'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 5) & (ATT['video_credit_risk'] == 'Low') & (
                ATT['ATT Revenue From Install'] > 0), ATT['ATT Revenue From Install'] + 25,
    ATT['ATT Revenue From Install'])
ATT['ATT Revenue From Install'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 6) & (ATT['video_credit_risk'] == 'Low') & (
                ATT['ATT Revenue From Install'] > 0), ATT['ATT Revenue From Install'] + 25,
    ATT['ATT Revenue From Install'])
ATT['ATT Revenue From Install'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 7) & (ATT['video_credit_risk'] == 'Low') & (
                ATT['ATT Revenue From Install'] > 0), ATT['ATT Revenue From Install'] + 25,
    ATT['ATT Revenue From Install'])
ATT['ATT Revenue From Install'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 8) & (ATT['video_credit_risk'] == 'Low') & (
                ATT['ATT Revenue From Install'] > 0), ATT['ATT Revenue From Install'] + 75,
    ATT['ATT Revenue From Install'])
ATT['ATT Revenue From Install'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 9) & (ATT['video_credit_risk'] == 'Low') & (
                ATT['ATT Revenue From Install'] > 0), ATT['ATT Revenue From Install'] + 75,
    ATT['ATT Revenue From Install'])
ATT['ATT Revenue From Install'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 10) & (ATT['video_credit_risk'] == 'Low') & (
                ATT['ATT Revenue From Install'] > 0), ATT['ATT Revenue From Install'] + 75,
    ATT['ATT Revenue From Install'])

############ ATT Low risk Video Spiff

ATT['Total Value'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 4) & (ATT['video_credit_risk'] == 'Low') & (ATT['Total Value'] > 0),
    ATT['Total Value'] + 25, ATT['Total Value'])
ATT['Total Value'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 5) & (ATT['video_credit_risk'] == 'Low') & (ATT['Total Value'] > 0),
    ATT['Total Value'] + 25, ATT['Total Value'])
ATT['Total Value'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 6) & (ATT['video_credit_risk'] == 'Low') & (ATT['Total Value'] > 0),
    ATT['Total Value'] + 25, ATT['Total Value'])
ATT['Total Value'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 7) & (ATT['video_credit_risk'] == 'Low') & (ATT['Total Value'] > 0),
    ATT['Total Value'] + 25, ATT['Total Value'])
ATT['Total Value'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 8) & (ATT['video_credit_risk'] == 'Low') & (ATT['Total Value'] > 0),
    ATT['Total Value'] + 75, ATT['Total Value'])
ATT['Total Value'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 9) & (ATT['video_credit_risk'] == 'Low') & (ATT['Total Value'] > 0),
    ATT['Total Value'] + 75, ATT['Total Value'])
ATT['Total Value'] = np.where(
    (ATT['Year'] == 2022) & (ATT['Month'] == 10) & (ATT['video_credit_risk'] == 'Low') & (ATT['Total Value'] > 0),
    ATT['Total Value'] + 75, ATT['Total Value'])

## DTV
DTV = pd.merge(DTV, Video_table, left_on=['Provider-Sales', 'creditRisk'], right_on=['Provider-Sales', 'packageName'],
               how='left')
DTV = DTV[
    (DTV['orderDate'] >= DTV['start date']) & (DTV['orderDate'] <= DTV['end date']) | (DTV['start date'].isnull())]
DTV = DTV.rename(columns={'Commission from Provider': 'videoValue'})
DTV['videoValue'] = DTV['videoValue'].fillna(0)
del DTV['start date']
del DTV['end date']

# DTV Extra 1
DTV['videoValue_extra1'] = 0
DTV.loc[DTV['package'].str.contains('Choice|Premier|Ultimate', na=False, case=False), ['videoValue_extra1']] = 50
# DTV.loc[DTV['orderDate']>='2023-03-13',['videoValue_extra1']]=0


#### Get the total video Value
DTV['videoValue'] = DTV['videoValue'] + DTV['videoValue_extra1']

# DTV.loc[(DTV['orderDate']>='2023-03-13') & (DTV['creditRisk']=='Low'), ['videoValue'] ] = DTV['videoValue']  + 40
DTV.loc[(DTV['orderDate'] >= '2023-04-01') & (DTV['orderDate'] <= '2023-04-30') & (DTV['creditRisk'] == 'Low'), [
    'videoValue']] = DTV['videoValue'] + 75
DTV.loc[(DTV['orderDate'] >= '2023-09-01') & (DTV['orderDate'] <= '2024-03-31') & (DTV['creditRisk'] == 'Low'), [
    'videoValue']] = DTV['videoValue'] + 50

# Addon Skip for now 2022/07/21
DTV['addonValue'] = 0.0
DTV['Total Value'] = DTV['videoValue']
DTV['Month'] = pd.to_datetime(DTV['orderDate']).dt.month
DTV['Year'] = pd.to_datetime(DTV['orderDate']).dt.year

# Rename Revenue from sale for each product
DTV['DTV videoValue'] = DTV['videoValue']
DTV['DTV addonValue'] = DTV['addonValue']

## Combine Install date and status
DTV.loc[(~(DTV['installDate'].isnull()) & (DTV['installDate'] != None) & (DTV['orderLineStatus'] != 'Disconnected')), [
    'orderLineStatus']] = 'Active_updated'

## Combine Disconnect date and status
DTV.loc[
    (~(DTV['disconnectDate'].isnull()) & (DTV['disconnectDate'] != None)), ['orderLineStatus']] = 'Disconnected_updated'

# DTV Chargeback
DTV['DTV Chargeback'] = 0
DTV.loc[(DTV['orderLineStatus'] == 'Disconnected_updated') | (DTV['orderLineStatus'] == 'Disconnected'), [
    'DTV Chargeback']] = DTV['DTV Chargeback'] + DTV['videoValue']

# DTV Disconnect Unit
DTV['DTV Disconnect Unit'] = 0
DTV.loc[(DTV['orderLineStatus'] == 'Disconnected_updated') | (DTV['orderLineStatus'] == 'Disconnected'), [
    'DTV Disconnect Unit']] = DTV['DTV Disconnect Unit'] + 1

### DTV Revenue from install
DTV['DTV Revenue From Install'] = 0
DTV.loc[(DTV['orderLineStatus'] == 'Active_updated') | (DTV['orderLineStatus'].isin(['Active', 'Installed'])), [
    'DTV Revenue From Install']] = DTV['DTV Revenue From Install'] + DTV['videoValue']

# SPC
SPC['Month'] = pd.to_datetime(SPC['orderDate']).dt.month
SPC['Year'] = pd.to_datetime(SPC['orderDate']).dt.year

# NEW for spc division
SPC.loc[SPC['SalesIdDivision'].isnull(), 'SalesIdDivision'] = 'DIGITAL'
SPC.loc[
    (SPC['SalesIdDivision'] == 'DIGITAL') & (SPC['SalesIdDivisionTier'].isnull()), 'SalesIdDivisionTier'] = 'Digital'
SPC.loc[(SPC['SalesIdDivision'] == 'RETAIL') & (
    SPC['SalesIdDivisionTier'].isnull()), 'SalesIdDivisionTier'] = 'Non Residual'

## SPC Internet
SPC = pd.merge(SPC, Internet_table, left_on=['Provider-Sales', 'InternetPackage', 'SalesIdDivisionTier'],
               right_on=['Provider-Sales', 'packageName', 'SalesIdDivisionTier'], how='left')
SPC = SPC[
    (pd.to_datetime(SPC['orderDate']) >= SPC['start date']) & (pd.to_datetime(SPC['orderDate']) <= SPC['end date']) | (
        SPC['start date'].isnull())]
SPC = SPC.rename(columns={'Commission from Provider': 'internetValue'})
SPC['internetValue'] = SPC['internetValue'].fillna(0)
del SPC['start date']
del SPC['end date']

SPC = pd.merge(SPC, proration, left_on=['Provider-Sales', 'SalesIdDivisionTier'],
               right_on=['Provider-Sales', 'SalesIdDivisionTier'], how='left')
SPC = SPC[
    (pd.to_datetime(SPC['orderDate']) >= SPC['start date']) & (pd.to_datetime(SPC['orderDate']) <= SPC['end date']) | (
        SPC['start date'].isnull())]
del SPC['start date']
del SPC['end date']

SPC['internetValue'] = SPC['internetValue'].fillna(0) * SPC['Internet proration'].fillna(0)

## SPC Video
SPC = pd.merge(SPC, Video_table, left_on=['Provider-Sales', 'VideoPackage', 'SalesIdDivisionTier'],
               right_on=['Provider-Sales', 'packageName', 'SalesIdDivisionTier'], how='left')
SPC = SPC[
    (pd.to_datetime(SPC['orderDate']) >= SPC['start date']) & (pd.to_datetime(SPC['orderDate']) <= SPC['end date']) | (
        SPC['start date'].isnull())]
SPC = SPC.rename(columns={'Commission from Provider': 'videoValue'})
SPC['videoValue'] = SPC['videoValue'].fillna(0)
del SPC['start date']
del SPC['end date']

SPC['videoValue'] = SPC['videoValue'].fillna(0) * SPC['Video proration'].fillna(0)

## SPC Phone
SPC = pd.merge(SPC, Phone_table, left_on=['Provider-Sales', 'PhonePackage', 'SalesIdDivisionTier'],
               right_on=['Provider-Sales', 'packageName', 'SalesIdDivisionTier'], how='left')
SPC = SPC[
    (pd.to_datetime(SPC['orderDate']) >= SPC['start date']) & (pd.to_datetime(SPC['orderDate']) <= SPC['end date']) | (
        SPC['start date'].isnull())]
SPC = SPC.rename(columns={'Commission from Provider': 'phoneValue'})
SPC['phoneValue'] = SPC['phoneValue'].fillna(0)
del SPC['start date']
del SPC['end date']

SPC['phoneValue'] = SPC['phoneValue'].fillna(0) * SPC['Phone proration'].fillna(0)

## SPC Mobile
# Mobile Old and new commission
SPC['<=Dec28'] = np.where(SPC['orderDate'] <= '2022-12-28', 1, 0)
SPC['mobileValue_sold'] = 0

# Loop that looks through all date and assign correct mobile values
SPC.loc[(pd.to_datetime(SPC['orderDate']) <= pd.to_datetime('2022-12-28')), ['mobileValue_sold']] = 150 * SPC[
    'MobileLines']
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (SPC['MobileLines'] == 1), [
    'mobileValue_sold']] = 100
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (SPC['MobileLines'] == 2), [
    'mobileValue_sold']] = 300
# digital division got $150 for third line
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (SPC['SalesIdDivision'] == 'DIGITAL') & (
            SPC['MobileLines'] > 2), ['mobileValue_sold']] = 300 + (SPC['MobileLines'] - 2) * 150
# Retail division got $150 for third line until 03/01/2024, then $200 for third line moving forward
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (
            pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2024-08-01')) & (SPC['SalesIdDivision'] == 'RETAIL') & (
                    SPC['MobileLines'] > 2), ['mobileValue_sold']] = 300 + (SPC['MobileLines'] - 2) * 150
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-08-01')) & (SPC['SalesIdDivision'] == 'RETAIL') & (
            SPC['MobileLines'] > 2), ['mobileValue_sold']] = 300 + (SPC['MobileLines'] - 2) * 200

## Internet Spiff
SPC.loc[(SPC['Month'] == 6) & (SPC['Year'] == 2023), ['internetValue']] = SPC['internetValue'] + 30

# SPC Total Value (no mobile since mobile has no churn)
SPC['Total Value'] = SPC['internetValue'] + SPC['videoValue'] + SPC['phoneValue']  # +SPC['mobileValue']

# SPC Churn Rate
# SPC.loc[(pd.to_datetime(SPC['orderDate'])>= pd.to_datetime('2022-05-01') ) & (pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2022-08-01') ) ,['Total Value']] = SPC['Total Value']  * (1-0.18)
# digital division still following 7% charge back
# SPC.loc[(pd.to_datetime(SPC['orderDate'])>= pd.to_datetime('2022-08-01') ) &  (SPC['SalesIdDivision']=='DIGITAL')   ,['Total Value']] = SPC['Total Value']  * (1-0.07)

# retail division after 07/29 are using 1:1 cb, so here we cannot pre-remove churn
# SPC.loc[(pd.to_datetime(SPC['orderDate'])>= pd.to_datetime('2022-08-01') ) & (pd.to_datetime(SPC['orderDate'])< pd.to_datetime('2024-07-29')) & (SPC['SalesIdDivision']=='RETAIL')  ,['Total Value']] = SPC['Total Value']  * (1-0.07)

## since we dont effective date of the 1:1 cb
# SPC.loc[(pd.to_datetime(SPC['orderDate'])>= pd.to_datetime('2024-07-29') ) &  (SPC['SalesIdDivision']=='RETAIL')  ,['Total Value']] = SPC['Total Value'] * (1-0.07)

# SPC.loc[(pd.to_datetime(SPC['orderDate'])>= pd.to_datetime('2024-07-29') ) &  (SPC['SalesIdDivision']=='RETAIL')  ,['Total Value']] = SPC['Total Value']


SPC['Total Value'] = SPC['Total Value'] + SPC['mobileValue_sold']

# Rename
# retail division after 07/29 are using 1:1 cb, so here we cannot pre-remove churn
## we dont know when this will in effective
SPC['SPC internetValue'] = SPC['internetValue']

SPC['SPC videoValue'] = SPC['videoValue']

SPC['SPC phoneValue'] = SPC['phoneValue']

SPC['SPC mobileValue'] = SPC['mobileValue_sold']

## Combine Install date and status
# Digital all time and retail division prior to 2024-07-29
# Internet
SPC.loc[(pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2024-07-29')) & (SPC['InternetOrderLineStatus'].isin(
    ['Active', 'Installed', 'Disconnected'])), 'InternetOrderLineStatus'] = 'Installed_updated'
# SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (SPC['InternetOrderLineStatus'].isin(['Active', 'Installed'])) &  (SPC['SalesIdDivision']=='RETAIL'), 'InternetOrderLineStatus'] = 'Installed_updated'
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (
    SPC['InternetOrderLineStatus'].isin(['Active', 'Installed', 'Disconnected'])) & (
                    SPC['SalesIdDivision'] == 'RETAIL'), 'InternetOrderLineStatus'] = 'Installed_updated'
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (
    SPC['InternetOrderLineStatus'].isin(['Active', 'Installed', 'Disconnected'])) & (
                    SPC['SalesIdDivision'] == 'DIGITAL'), 'InternetOrderLineStatus'] = 'Installed_updated'

# Video
SPC.loc[(pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2024-07-29')) & (SPC['VideoOrderLineStatus'].isin(
    ['Active', 'Installed', 'Disconnected'])), 'VideoOrderLineStatus'] = 'Installed_updated'
# SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (SPC['VideoOrderLineStatus'].isin(['Active', 'Installed'])) &  (SPC['SalesIdDivision']=='RETAIL'), 'VideoOrderLineStatus'] = 'Installed_updated'
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (
    SPC['VideoOrderLineStatus'].isin(['Active', 'Installed', 'Disconnected'])) & (
                    SPC['SalesIdDivision'] == 'RETAIL'), 'VideoOrderLineStatus'] = 'Installed_updated'
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (
    SPC['VideoOrderLineStatus'].isin(['Active', 'Installed', 'Disconnected'])) & (
                    SPC['SalesIdDivision'] == 'DIGITAL'), 'VideoOrderLineStatus'] = 'Installed_updated'

# Phone
SPC.loc[(pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2024-07-29')) & (SPC['PhoneOrderLineStatus'].isin(
    ['Active', 'Installed', 'Disconnected'])), 'PhoneOrderLineStatus'] = 'Installed_updated'
# SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (SPC['PhoneOrderLineStatus'].isin(['Active', 'Installed'])) &  (SPC['SalesIdDivision']=='RETAIL'), 'PhoneOrderLineStatus'] = 'Installed_updated'
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (
    SPC['PhoneOrderLineStatus'].isin(['Active', 'Installed', 'Disconnected'])) & (
                    SPC['SalesIdDivision'] == 'RETAIL'), 'PhoneOrderLineStatus'] = 'Installed_updated'
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-07-29')) & (
    SPC['PhoneOrderLineStatus'].isin(['Active', 'Installed', 'Disconnected'])) & (
                    SPC['SalesIdDivision'] == 'DIGITAL'), 'PhoneOrderLineStatus'] = 'Installed_updated'

### SPC Revenue from install
SPC['SPC Revenue From Install'] = 0
SPC.loc[(SPC['InternetOrderLineStatus'].isin(['Installed_updated'])), ['SPC Revenue From Install']] = SPC[
                                                                                                          'SPC Revenue From Install'] + \
                                                                                                      SPC[
                                                                                                          'internetValue']
SPC.loc[(SPC['VideoOrderLineStatus'].isin(['Installed_updated'])), ['SPC Revenue From Install']] = SPC[
                                                                                                       'SPC Revenue From Install'] + \
                                                                                                   SPC['videoValue']
SPC.loc[(SPC['PhoneOrderLineStatus'].isin(['Installed_updated'])), ['SPC Revenue From Install']] = SPC[
                                                                                                       'SPC Revenue From Install'] + \
                                                                                                   SPC['phoneValue']

# Mobile Value Installed

SPC['mobileValue_installed'] = 0

# Loop that looks through all date and assign correct mobile values
SPC.loc[(pd.to_datetime(SPC['orderDate']) <= pd.to_datetime('2022-12-28')) & (
    SPC['MobileOrderLineStatus'].isin(['Installed_updated'])), ['mobileValue_installed']] = 150 * SPC['MobileLines']
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (SPC['MobileLines'] == 1) & (
    SPC['MobileOrderLineStatus'].isin(['Installed_updated'])), ['mobileValue_installed']] = 100
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (SPC['MobileLines'] == 2) & (
    SPC['MobileOrderLineStatus'].isin(['Installed_updated'])), ['mobileValue_installed']] = 300
# digital division got $150 for third line
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (SPC['SalesIdDivision'] == 'DIGITAL') & (
            SPC['MobileLines'] > 2) & (
            SPC['MobileOrderLineStatus'].isin(['Active', 'Installed', 'Installed_updated'])), [
    'mobileValue_installed']] = 300 + (SPC['MobileLines'] - 2) * 150
# Retail division got $150 for third line until 03/01/2024, then $200 for third line moving forward
SPC.loc[(pd.to_datetime(SPC['orderDate']) > pd.to_datetime('2022-12-28')) & (
            pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2024-08-01')) & (SPC['SalesIdDivision'] == 'RETAIL') & (
                    SPC['MobileLines'] > 2), ['mobileValue_installed']] = 300 + (SPC['MobileLines'] - 2) * 150
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2024-08-01')) & (SPC['SalesIdDivision'] == 'RETAIL') & (
            SPC['MobileLines'] > 2), ['mobileValue_installed']] = 300 + (SPC['MobileLines'] - 2) * 200

# Churn for Rev from installedSPC.loc[(pd.to_datetime(SPC['orderDate'])>= pd.to_datetime('2022-05-01') ) & (pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2022-08-01') ) ,['Total Value']] = SPC['Total Value']  * (1-0.18)
# digital division still following 7% charge back
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2022-08-01')) & (SPC['SalesIdDivision'] == 'DIGITAL'), [
    'SPC Revenue From Install']] = SPC['SPC Revenue From Install'] * (1 - 0.07)
# retail division after 07/29 are using 1:1 cb, so here we cannot pre-remove churn
SPC.loc[(pd.to_datetime(SPC['orderDate']) >= pd.to_datetime('2022-08-01')) & (
            pd.to_datetime(SPC['orderDate']) < pd.to_datetime('2024-07-29')) & (SPC['SalesIdDivision'] == 'RETAIL'), [
    'SPC Revenue From Install']] = SPC['SPC Revenue From Install'] * (1 - 0.07)

SPC['SPC Revenue From Install'] = SPC['SPC Revenue From Install'] + SPC['mobileValue_installed']

# Altice
## Altice Internet
OPT_SUD = pd.merge(OPT_SUD, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
                   right_on=['Provider-Sales', 'packageName'], how='left')
OPT_SUD = OPT_SUD[(OPT_SUD['orderDate'] >= OPT_SUD['start date']) & (OPT_SUD['orderDate'] <= OPT_SUD['end date']) | (
    OPT_SUD['start date'].isnull())]
OPT_SUD = OPT_SUD.rename(columns={'Commission from Provider': 'internetValue'})
OPT_SUD['internetValue'] = OPT_SUD['internetValue'].fillna(0)
del OPT_SUD['start date']
del OPT_SUD['end date']

## Altice Video
# OPT_SUD['videoValue'] = np.where(~OPT_SUD['videoPackage'].isnull(),90,0)
## Altice Video commission changes on 04/01
OPT_SUD = pd.merge(OPT_SUD, Video_table, left_on=['Provider-Sales', 'videoPackage'],
                   right_on=['Provider-Sales', 'packageName'], how='left')
OPT_SUD = OPT_SUD[(pd.to_datetime(OPT_SUD['orderDate']) >= OPT_SUD['start date']) & (
            pd.to_datetime(OPT_SUD['orderDate']) <= OPT_SUD['end date']) | (OPT_SUD['start date'].isnull())]
OPT_SUD = OPT_SUD.rename(columns={'Commission from Provider': 'videoValue'})
OPT_SUD['videoValue'] = OPT_SUD['videoValue'].fillna(0)
del OPT_SUD['start date']
del OPT_SUD['end date']

## Altice Phone
OPT_SUD['phoneValue'] = np.where(~OPT_SUD['phonePackage'].isnull(), 90, 0)

## Altice Mobile
OPT_SUD['mobileValue'] = np.where(~OPT_SUD['mobilePackage'].isnull(), 100 * OPT_SUD['mobileSoldUnits'], 0)

# Total Value
OPT_SUD['Total Value'] = OPT_SUD['internetValue'] + OPT_SUD['videoValue'] + OPT_SUD['phoneValue'] + OPT_SUD[
    'mobileValue']

# rename
OPT_SUD['ALTICE internetValue'] = OPT_SUD['internetValue']
OPT_SUD['ALTICE videoValue'] = OPT_SUD['videoValue']
OPT_SUD['ALTICE phoneValue'] = OPT_SUD['phoneValue']
OPT_SUD['ALTICE mobileValue'] = OPT_SUD['mobileValue']

OPT_SUD['ALTICE addonValue'] = 0
OPT_SUD.loc[
    (pd.to_datetime(OPT_SUD['orderDate']) >= '2023-04-01') & (pd.to_datetime(OPT_SUD['orderDate']) <= '2024-03-31'), [
        'ALTICE addonValue']] = 160

OPT_SUD['Month'] = pd.to_datetime(OPT_SUD['orderDate']).dt.month
OPT_SUD['Year'] = pd.to_datetime(OPT_SUD['orderDate']).dt.year

# Altice Spiff
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 5) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 70, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 6) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 100, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 7) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 70, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 8) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 70, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 9) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 160, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 10) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 160, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 11) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 160, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 12) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 10, OPT_SUD['Total Value'])

OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] == 1) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 10, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] == 2) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 10, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] == 3) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 10, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where(
    (OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] > 3) & (OPT_SUD['Month'] <= 12) & (OPT_SUD['Total Value'] > 0),
    OPT_SUD['Total Value'] + 160, OPT_SUD['Total Value'])
OPT_SUD['Total Value'] = np.where((OPT_SUD['Year'] >= 2024) & (OPT_SUD['Month'] <= 3) & (OPT_SUD['Total Value'] > 0),
                                  OPT_SUD['Total Value'] + 160, OPT_SUD['Total Value'])

## Combine Install date and status
OPT_SUD.loc[(~(OPT_SUD['internetInstallDate'].isnull()) & ~(OPT_SUD['internetInstallDate'] == None) & (
            OPT_SUD['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Installed_updated'
OPT_SUD.loc[(~(OPT_SUD['videoInstallDate'].isnull()) & ~(OPT_SUD['videoInstallDate'] == None) & (
            OPT_SUD['videoOrderLineStatus'] != 'Disconnected')), ['videoOrderLineStatus']] = 'Installed_updated'
OPT_SUD.loc[(~(OPT_SUD['phoneInstallDate'].isnull()) & ~(OPT_SUD['phoneInstallDate'] == None) & (
            OPT_SUD['phoneOrderLineStatus'] != 'Disconnected')), ['phoneOrderLineStatus']] = 'Installed_updated'
OPT_SUD.loc[(~(OPT_SUD['mobileInstallDate'].isnull()) & ~(OPT_SUD['mobileInstallDate'] == None) & (
            OPT_SUD['mobileOrderLineStatus'] != 'Disconnected')), ['mobileOrderLineStatus']] = 'Installed_updated'

## Combine Disconnect date and status
OPT_SUD.loc[(~(OPT_SUD['internetDisconnectDate'].isnull()) & ~(OPT_SUD['internetDisconnectDate'] == None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'
OPT_SUD.loc[(~(OPT_SUD['videoDisconnectDate'].isnull()) & ~(OPT_SUD['videoDisconnectDate'] == None)), [
    'videoOrderLineStatus']] = 'Disconnected_updated'
OPT_SUD.loc[(~(OPT_SUD['phoneDisconnectDate'].isnull()) & ~(OPT_SUD['phoneDisconnectDate'] == None)), [
    'phoneOrderLineStatus']] = 'Disconnected_updated'
OPT_SUD.loc[(~(OPT_SUD['mobileDisconnectDate'].isnull()) & ~(OPT_SUD['mobileDisconnectDate'] == None)), [
    'mobileOrderLineStatus']] = 'Disconnected_updated'

### OPT_SUD Chargeback
OPT_SUD['OPT_SUD Chargeback'] = 0
OPT_SUD.loc[(OPT_SUD['internetOrderLineStatus'] == 'Disconnected_updated') | (
            OPT_SUD['internetOrderLineStatus'] == 'Disconnected'), ['OPT_SUD Chargeback']] = OPT_SUD[
                                                                                                 'OPT_SUD Chargeback'] + \
                                                                                             OPT_SUD['internetValue']
OPT_SUD.loc[
    (OPT_SUD['videoOrderLineStatus'] == 'Disconnected_updated') | (OPT_SUD['videoOrderLineStatus'] == 'Disconnected'), [
        'OPT_SUD Chargeback']] = OPT_SUD['OPT_SUD Chargeback'] + OPT_SUD['videoValue']
OPT_SUD.loc[
    (OPT_SUD['phoneOrderLineStatus'] == 'Disconnected_updated') | (OPT_SUD['phoneOrderLineStatus'] == 'Disconnected'), [
        'OPT_SUD Chargeback']] = OPT_SUD['OPT_SUD Chargeback'] + OPT_SUD['phoneValue']
OPT_SUD.loc[(OPT_SUD['mobileOrderLineStatus'] == 'Disconnected_updated') | (
            OPT_SUD['mobileOrderLineStatus'] == 'Disconnected'), ['OPT_SUD Chargeback']] = OPT_SUD[
                                                                                               'OPT_SUD Chargeback'] + \
                                                                                           OPT_SUD['mobileValue']

### OPT_SUD Disconnect Unit
OPT_SUD['OPT_SUD Disconnect Unit'] = 0
OPT_SUD.loc[(OPT_SUD['internetOrderLineStatus'] == 'Disconnected_updated') | (
            OPT_SUD['internetOrderLineStatus'] == 'Disconnected'), ['OPT_SUD Disconnect Unit']] = OPT_SUD[
                                                                                                      'OPT_SUD Disconnect Unit'] + 1
OPT_SUD.loc[
    (OPT_SUD['videoOrderLineStatus'] == 'Disconnected_updated') | (OPT_SUD['videoOrderLineStatus'] == 'Disconnected'), [
        'OPT_SUD Disconnect Unit']] = OPT_SUD['OPT_SUD Disconnect Unit'] + 1
OPT_SUD.loc[
    (OPT_SUD['phoneOrderLineStatus'] == 'Disconnected_updated') | (OPT_SUD['phoneOrderLineStatus'] == 'Disconnected'), [
        'OPT_SUD Disconnect Unit']] = OPT_SUD['OPT_SUD Disconnect Unit'] + 1
OPT_SUD.loc[(OPT_SUD['mobileOrderLineStatus'] == 'Disconnected_updated') | (
            OPT_SUD['mobileOrderLineStatus'] == 'Disconnected'), ['OPT_SUD Disconnect Unit']] = OPT_SUD[
                                                                                                    'OPT_SUD Disconnect Unit'] + \
                                                                                                OPT_SUD[
                                                                                                    'mobileSoldUnits']

### OPT_SUD Revenue from install
OPT_SUD['OPT_SUD Revenue From Install'] = 0
OPT_SUD.loc[(OPT_SUD['internetOrderLineStatus'] == 'Installed_updated') | (
    OPT_SUD['internetOrderLineStatus'].isin(['Active', 'Installed'])), ['OPT_SUD Revenue From Install']] = OPT_SUD[
                                                                                                               'OPT_SUD Revenue From Install'] + \
                                                                                                           OPT_SUD[
                                                                                                               'internetValue']
OPT_SUD.loc[(OPT_SUD['videoOrderLineStatus'] == 'Installed_updated') | (
    OPT_SUD['videoOrderLineStatus'].isin(['Active', 'Installed'])), ['OPT_SUD Revenue From Install']] = OPT_SUD[
                                                                                                            'OPT_SUD Revenue From Install'] + \
                                                                                                        OPT_SUD[
                                                                                                            'videoValue']
OPT_SUD.loc[(OPT_SUD['phoneOrderLineStatus'] == 'Installed_updated') | (
    OPT_SUD['phoneOrderLineStatus'].isin(['Active', 'Installed'])), ['OPT_SUD Revenue From Install']] = OPT_SUD[
                                                                                                            'OPT_SUD Revenue From Install'] + \
                                                                                                        OPT_SUD[
                                                                                                            'phoneValue']
OPT_SUD.loc[(OPT_SUD['mobileOrderLineStatus'] == 'Installed_updated') | (
    OPT_SUD['mobileOrderLineStatus'].isin(['Active', 'Installed'])), ['OPT_SUD Revenue From Install']] = OPT_SUD[
                                                                                                             'OPT_SUD Revenue From Install'] + \
                                                                                                         OPT_SUD[
                                                                                                             'mobileValue']

# comment for now
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 5) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 10, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 6) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 70, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 7) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 25, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 8) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 10, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 9) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 160, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 10) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 160, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 11) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 160, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2022) & (OPT_SUD['Month'] == 12) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 10, OPT_SUD['OPT_SUD Revenue From Install'])

OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] == 1) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 10, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] == 2) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 10, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] == 3) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 10, OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2023) & (OPT_SUD['Month'] > 3) & (OPT_SUD['Month'] <= 12) & (
                OPT_SUD['OPT_SUD Revenue From Install'] > 0), OPT_SUD['OPT_SUD Revenue From Install'] + 160,
    OPT_SUD['OPT_SUD Revenue From Install'])
OPT_SUD['OPT_SUD Revenue From Install'] = np.where(
    (OPT_SUD['Year'] == 2024) & (OPT_SUD['Month'] <= 3) & (OPT_SUD['OPT_SUD Revenue From Install'] > 0),
    OPT_SUD['OPT_SUD Revenue From Install'] + 160, OPT_SUD['OPT_SUD Revenue From Install'])

### VIASAT
### Viasat Internet
Viasat['internetValue'] = np.where(~Viasat['internetPackage'].isnull(), 260, 0)

### Visasat Phone
Viasat['phoneValue'] = np.where(~Viasat['phonePackage'].isnull(), 50, 0)

# Viasat Addon
Viasat = pd.merge(Viasat, Addon_table, left_on=['Provider-Sales', 'addonsPackage'],
                  right_on=['Provider-Sales', 'packageName'], how='left')
Viasat = Viasat[(Viasat['orderDate'] >= Viasat['start date']) & (Viasat['orderDate'] <= Viasat['end date']) | (
    Viasat['start date'].isnull())]
Viasat = Viasat.rename(columns={'Commission from Provider': 'addonsValue'})
Viasat['addonsValue'] = Viasat['addonsValue'].fillna(0)
del Viasat['start date']
del Viasat['end date']

# Viasta Internet Spiff
Viasat.loc[Viasat['orderDate'] >= '2023-01-15', ['internetValue']] = Viasat['internetValue'] + 20

# Viasat Total Value
Viasat['Total Value'] = Viasat['internetValue'] + Viasat['phoneValue'] + Viasat['addonsValue']

# rename
Viasat['Viasat internetValue'] = Viasat['internetValue']
Viasat['Viasat phoneValue'] = Viasat['phoneValue']
Viasat['Viasat addonsValue'] = Viasat['addonsValue']

## Combine Install date and status
Viasat.loc[(~(Viasat['internetInstallDate'].isnull()) & ~(Viasat['internetInstallDate'] == None) & (
            Viasat['internetOrderLineStatus'] != 'Cancelled')), ['internetOrderLineStatus']] = 'Completed_updated'
Viasat.loc[(~(Viasat['addonsInstallDate'].isnull()) & ~(Viasat['addonsInstallDate'] == None) & (
            Viasat['addonsOrderLineStatus'] != 'Cancelled')), ['addonsOrderLineStatus']] = 'Completed_updated'
Viasat.loc[(~(Viasat['phoneInstallDate'].isnull()) & ~(Viasat['phoneInstallDate'] == None) & (
            Viasat['phoneOrderLineStatus'] != 'Cancelled')), ['phoneOrderLineStatus']] = 'Completed_updated'

## Combine Disconnect date and status
Viasat.loc[(~(Viasat['internetDisconnectDate'].isnull()) & ~(Viasat['internetDisconnectDate'] == None)), [
    'internetOrderLineStatus']] = 'Cancelled_updated'
Viasat.loc[(~(Viasat['addonsDisconnectDate'].isnull()) & ~(Viasat['addonsDisconnectDate'] == None)), [
    'addonsOrderLineStatus']] = 'Cancelled_updated'
Viasat.loc[(~(Viasat['phoneDisconnectDate'].isnull()) & ~(Viasat['phoneDisconnectDate'] == None)), [
    'phoneOrderLineStatus']] = 'Cancelled_updated'

### Viasat Revenue from install
Viasat['Viasat Revenue From Install'] = 0
Viasat.loc[
    (Viasat['internetOrderLineStatus'] == 'Completed_updated') | (Viasat['internetOrderLineStatus'] == 'Completed'), [
        'Viasat Revenue From Install']] = Viasat['Viasat Revenue From Install'] + Viasat['internetValue']
Viasat.loc[
    (Viasat['addonsOrderLineStatus'] == 'Completed_updated') | (Viasat['addonsOrderLineStatus'] == 'Completed'), [
        'Viasat Revenue From Install']] = Viasat['Viasat Revenue From Install'] + Viasat['addonsValue']
Viasat.loc[(Viasat['phoneOrderLineStatus'] == 'Completed_updated') | (Viasat['phoneOrderLineStatus'] == 'Completed'), [
    'Viasat Revenue From Install']] = Viasat['Viasat Revenue From Install'] + Viasat['phoneValue']

### Frontier
## Frontier Internet
Frontier = pd.merge(Frontier, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
                    right_on=['Provider-Sales', 'packageName'], how='left')
Frontier = Frontier[
    (Frontier['orderDate'] >= Frontier['start date']) & (Frontier['orderDate'] <= Frontier['end date']) | (
        Frontier['start date'].isnull())]
Frontier = Frontier.rename(columns={'Commission from Provider': 'internetValue'})
Frontier['internetValue'] = Frontier['internetValue'].fillna(0)
del Frontier['start date']
del Frontier['end date']

### Frontier Phone
Frontier['voiceValue'] = np.where(~Frontier['voicePackage'].isnull(), 150, 0)

## Frontier Wirless
Frontier = pd.merge(Frontier, Wireless_table, left_on=['Provider-Sales', 'wirelessPackage'],
                    right_on=['Provider-Sales', 'packageName'], how='left')
Frontier = Frontier[
    (Frontier['orderDate'] >= Frontier['start date']) & (Frontier['orderDate'] <= Frontier['end date']) | (
        Frontier['start date'].isnull())]
Frontier = Frontier.rename(columns={'Commission from Provider': 'wirelessValue'})
Frontier['wirelessValue'] = Frontier['wirelessValue'].fillna(0)
del Frontier['start date']
del Frontier['end date']

# Fronter Spiff
Frontier.loc[(Frontier['orderDate'] >= '2023-04-01') & (Frontier['orderDate'] <= '2023-06-30'), ['internetValue']] = \
Frontier['internetValue'] + 125

# Fronter Total Value
Frontier['Total Value'] = Frontier['internetValue'] + Frontier['voiceValue'] + Frontier['wirelessValue']

# Rename
Frontier['Frontier internetValue'] = Frontier['internetValue']
Frontier['Frontier voiceValue'] = Frontier['voiceValue']
Frontier['Frontier wirelessValue'] = Frontier['wirelessValue']

## Combine Install date and status
Frontier.loc[(~(Frontier['internetInstallDate'].isnull()) & ~(Frontier['internetInstallDate'] == None) & (
            Frontier['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Installed_updated'
Frontier.loc[(~(Frontier['voiceInstallDate'].isnull()) & ~(Frontier['voiceInstallDate'] == None) & (
            Frontier['voiceOrderLineStatus'] != 'Disconnected')), ['voiceOrderLineStatus']] = 'Installed_updated'
Frontier.loc[(~(Frontier['wirelessInstallDate'].isnull()) & ~(Frontier['wirelessInstallDate'] == None) & (
            Frontier['wirelessOrderLineStatus'] != 'Disconnected')), ['wirelessOrderLineStatus']] = 'Installed_updated'

## Combine Disconnect date and status
Frontier.loc[(~(Frontier['internetDisconnectDate'].isnull()) & ~(Frontier['internetDisconnectDate'] == None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'
Frontier.loc[(~(Frontier['voiceDisconnectDate'].isnull()) & ~(Frontier['voiceDisconnectDate'] == None)), [
    'voiceOrderLineStatus']] = 'Disconnected_updated'
Frontier.loc[(~(Frontier['wirelessDisconnectDate'].isnull()) & ~(Frontier['wirelessDisconnectDate'] == None)), [
    'wirelessOrderLineStatus']] = 'Disconnected_updated'

# Frontier Charge Back
Frontier['Frontier Charge Back'] = 0
Frontier.loc[(Frontier['internetOrderLineStatus'] == 'Disconnected_updated') | (
            Frontier['internetOrderLineStatus'] == 'Disconnected'), ['Frontier Charge Back']] = Frontier[
                                                                                                    'Frontier Charge Back'] + \
                                                                                                Frontier[
                                                                                                    'internetValue']
Frontier.loc[(Frontier['voiceOrderLineStatus'] == 'Disconnected_updated') | (
            Frontier['voiceOrderLineStatus'] == 'Disconnected'), ['Frontier Charge Back']] = Frontier[
                                                                                                 'Frontier Charge Back'] + \
                                                                                             Frontier['voiceValue']
Frontier.loc[(Frontier['wirelessOrderLineStatus'] == 'Disconnected_updated') | (
            Frontier['wirelessOrderLineStatus'] == 'Disconnected'), ['Frontier Charge Back']] = Frontier[
                                                                                                    'Frontier Charge Back'] + \
                                                                                                Frontier[
                                                                                                    'wirelessValue']

# Frontier Disconnect Unit
Frontier['Frontier Disconnect Unit'] = 0
Frontier.loc[(Frontier['internetOrderLineStatus'] == 'Disconnected_updated') | (
            Frontier['internetOrderLineStatus'] == 'Disconnected'), ['Frontier Disconnect Unit']] = Frontier[
                                                                                                        'Frontier Disconnect Unit'] + 1
Frontier.loc[(Frontier['voiceOrderLineStatus'] == 'Disconnected_updated') | (
            Frontier['voiceOrderLineStatus'] == 'Disconnected'), ['Frontier Disconnect Unit']] = Frontier[
                                                                                                     'Frontier Disconnect Unit'] + 1
Frontier.loc[(Frontier['wirelessOrderLineStatus'] == 'Disconnected_updated') | (
            Frontier['wirelessOrderLineStatus'] == 'Disconnected'), ['Frontier Disconnect Unit']] = Frontier[
                                                                                                        'Frontier Disconnect Unit'] + 1

### Frontier Revenue from install
Frontier['Frontier Revenue From Install'] = 0
Frontier.loc[(Frontier['internetOrderLineStatus'] == 'Installed_updated') | (
            Frontier['internetOrderLineStatus'] == 'Installed'), ['Frontier Revenue From Install']] = Frontier[
                                                                                                          'Frontier Revenue From Install'] + \
                                                                                                      Frontier[
                                                                                                          'internetValue']
Frontier.loc[(Frontier['voiceOrderLineStatus'] == 'Installed_updated') | (
    Frontier['voiceOrderLineStatus'].isin(['Active', 'Installed'])), ['Frontier Revenue From Install']] = Frontier[
                                                                                                              'Frontier Revenue From Install'] + \
                                                                                                          Frontier[
                                                                                                              'voiceValue']
Frontier.loc[(Frontier['wirelessOrderLineStatus'] == 'Installed_updated') | (
    Frontier['wirelessOrderLineStatus'].isin(['Active', 'Installed'])), ['Frontier Revenue From Install']] = Frontier[
                                                                                                                 'Frontier Revenue From Install'] + \
                                                                                                             Frontier[
                                                                                                                 'wirelessValue']

#### VIVINT
# apply function to create a new 'RMR' column
Vivint['RMR'] = Vivint['package'].astype(str).apply(lambda x: ' '.join(re.findall(r'\$\d+\.*\d*', x)) if x else '')

## Vivint Security
Vivint = pd.merge(Vivint, Security_table, left_on=['Provider-Sales', 'RMR'], right_on=['Provider-Sales', 'packageName'],
                  how='left')
Vivint = Vivint[(Vivint['orderDate'] >= Vivint['start date']) & (Vivint['orderDate'] <= Vivint['end date']) | (
    Vivint['start date'].isnull())]
Vivint = Vivint.rename(columns={'Commission from Provider': 'packageValue'})
Vivint['packageValue'] = Vivint['packageValue'].fillna(0)
del Vivint['start date']
del Vivint['end date']

# DIY package
Vivint.loc[(Vivint['orderedPackage'].str.contains('diy', na=False, case=False)) & (
    ~(Vivint['package'].str.contains('BPO Package', na=False, case=False))), ['packageValue']] = 150

# FCO package
Vivint.loc[(Vivint['orderedPackage'].str.contains('FCO', na=False, case=False)) & (
    ~(Vivint['package'].str.contains('BPO Package', na=False, case=False))), ['packageValue']] = 100

# Total Value
Vivint['Total Value'] = Vivint['packageValue']

# Rename
Vivint['Vivint packageValue'] = Vivint['packageValue']

## Combine Install date and status
# Vivint.loc[(~(Vivint['installDate'].isnull())&~(Vivint['installDate']== None)&(Vivint['orderLineStatus']!='Disconnected')),['orderLineStatus']]='Active_updated'

## Combine Disconnect date and status
# Vivint.loc[(~(Vivint['disconnectDate'].isnull())&~(Vivint['disconnectDate']== None)),['orderLineStatus']]='Disconnected_updated'

### Vivint Revenue from install
Vivint['Vivint Revenue From Install'] = 0
# effective 2024/02/19 we are on paid by unit SOLD, each fish is $350, FCO is $100
Vivint.loc[(Vivint['orderLineStatus'].isin(['Active', 'Installed'])) | (Vivint['orderDate'] >= '2024-03-01'), [
    'Vivint Revenue From Install']] = Vivint['Vivint Revenue From Install'] + Vivint['packageValue']

#### ALDER
Alder['credit'] = Alder['credit'].astype(str)
Alder['credit'] = Alder['credit'].str.replace(' ', '')

# Alder Security Package
Alder = pd.merge(Alder, Security_table, left_on=['Provider-Sales', 'credit'],
                 right_on=['Provider-Sales', 'packageName'], how='left')
Alder = Alder[(Alder['orderDate'] >= Alder['start date']) & (Alder['orderDate'] <= Alder['end date']) | (
    Alder['start date'].isnull())]
Alder = Alder.rename(columns={'Commission from Provider': 'packageValue'})
Alder['packageValue'] = Alder['packageValue'].fillna(0)
del Alder['start date']
del Alder['end date']

# Alder Total Value
Alder['Total Value'] = Alder['packageValue']

# Rename
Alder['Alder packageValue'] = Alder['Total Value']

## Combine Install date and status
Alder.loc[(~(Alder['installDate'].isnull()) & ~(Alder['installDate'] == None) & (
            Alder['orderLineStatus'] != 'Disconnected')), ['orderLineStatus']] = 'Installed_updated'

## Combine Disconnect date and status
Alder.loc[(~(Alder['disconnectDate'].isnull()) & ~(Alder['disconnectDate'] == None)), [
    'orderLineStatus']] = 'Disconnected_updated'

### Alder Chargeback
Alder['Alder Chargeback'] = 0
Alder.loc[(Alder['orderLineStatus'] == 'Disconnected_updated') | (Alder['orderLineStatus'] == 'Disconnected'), [
    'Alder Chargeback']] = Alder['Alder Chargeback'] + Alder['Total Value']

### Alder Disconnect Unit
Alder['Alder Disconnect Unit'] = 0
Alder.loc[(Alder['orderLineStatus'] == 'Disconnected_updated') | (Alder['orderLineStatus'] == 'Disconnected'), [
    'Alder Disconnect Unit']] = Alder['Alder Disconnect Unit'] + Alder['Total Value']

### Alder Revenue from install
Alder['Alder Revenue From Install'] = 0
Alder.loc[(Alder['orderLineStatus'] == 'Installed_updated') | (Alder['orderLineStatus'] == 'Installed'), [
    'Alder Revenue From Install']] = Alder['Alder Revenue From Install'] + Alder['Total Value']

### COMCAST
# $65 PER SOLD UNIT if unit sold <= 5000
COMCAST['Total Value'] = COMCAST['bundleUnit'] * 65

COMCAST['COMCAST Revenue From Install'] = COMCAST['bundleUnit'] * 65

# Rename
COMCAST['COMCAST internetValue'] = COMCAST['internetSoldUnits'] * 65
COMCAST['COMCAST voiceValue'] = COMCAST['voiceSoldUnits'] * 65
COMCAST['COMCAST videoValue'] = COMCAST['videoSoldUnits'] * 65
COMCAST['COMCAST securityValue'] = COMCAST['securitySoldUnits'] * 65

# New commission starting from 07/01
# Comcast Internet SOLD
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['internetPackage'].str.contains('Connect', na=False, case=False)), ['COMCAST internetValue']] = 59 * \
                                                                                                            COMCAST[
                                                                                                                'internetSoldUnits']
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['internetPackage'].str.contains('Connect More', na=False, case=False)), ['COMCAST internetValue']] = 79 * \
                                                                                                                 COMCAST[
                                                                                                                     'internetSoldUnits']
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['internetPackage'].str.contains('Fast', na=False, case=False)), ['COMCAST internetValue']] = 119 * COMCAST[
    'internetSoldUnits']
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['internetPackage'].str.contains('SuperFast', na=False, case=False)), ['COMCAST internetValue']] = 119 * \
                                                                                                              COMCAST[
                                                                                                                  'internetSoldUnits']
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['internetPackage'].str.contains('Gigabit', na=False, case=False)), ['COMCAST internetValue']] = 119 * \
                                                                                                            COMCAST[
                                                                                                                'internetSoldUnits']
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['internetPackage'].str.contains('Gigabit Extra', na=False, case=False)), ['COMCAST internetValue']] = 119 * \
                                                                                                                  COMCAST[
                                                                                                                      'internetSoldUnits']

# Comcast Video SOLD
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['videoPackage'].str.contains('Choice TV', na=False, case=False)), ['COMCAST videoValue']] = 31 * COMCAST[
    'videoSoldUnits']
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['videoPackage'].str.contains('Popular TV', na=False, case=False)), ['COMCAST videoValue']] = 31 * COMCAST[
    'videoSoldUnits']
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (
    COMCAST['videoPackage'].str.contains('Ultimate TV', na=False, case=False)), ['COMCAST videoValue']] = 31 * COMCAST[
    'videoSoldUnits']

# Comcast Voice SOLD
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & ~(COMCAST['voicePackage'].isnull()), [
    'COMCAST voiceValue']] = 28 * COMCAST['voiceSoldUnits']

# Comcast Bundle SOLD
COMCAST.loc[
    (COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (COMCAST['bundleUnit'] == 2), ['COMCAST internetValue']] = \
COMCAST['COMCAST internetValue'] + 39
COMCAST.loc[
    (COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (COMCAST['bundleUnit'] > 2), ['COMCAST internetValue']] = \
COMCAST['COMCAST internetValue'] + 47

# Comcast Total value  SOLD
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')), ['Total Value']] = COMCAST[
                                                                                           'COMCAST internetValue'] + \
                                                                                       COMCAST['COMCAST voiceValue'] + \
                                                                                       COMCAST['COMCAST videoValue'] + \
                                                                                       COMCAST['COMCAST securityValue']

# remove bundle bonus if not all be installed
# if sold 2 but installed less than 2, then remove double play bonus
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (COMCAST['bundleUnit'] == 2) & (
            COMCAST['installedUnits'] < 2), ['COMCAST internetValue']] = COMCAST['COMCAST internetValue'] - 39
# if sold 3 but installed 2, lower triple play bonus and reduce it to double play
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (COMCAST['bundleUnit'] > 2) & (
            COMCAST['installedUnits'] == 2), ['COMCAST internetValue']] = COMCAST['COMCAST internetValue'] - 8
# if sold 3 but installed 1, then remove double play bonus
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')) & (COMCAST['bundleUnit'] > 2) & (
            COMCAST['installedUnits'] < 2), ['COMCAST internetValue']] = COMCAST['COMCAST internetValue'] - 47

# Comcast total revenue from Installed
COMCAST.loc[(COMCAST['orderDate'] >= pd.to_datetime('2023-07-01')), ['COMCAST Revenue From Install']] = COMCAST[
                                                                                                            'COMCAST internetValue'] * \
                                                                                                        COMCAST[
                                                                                                            'internetInstalledunits'] + \
                                                                                                        COMCAST[
                                                                                                            'COMCAST voiceValue'] * \
                                                                                                        COMCAST[
                                                                                                            'voiceInstalledUnits'] + \
                                                                                                        COMCAST[
                                                                                                            'COMCAST videoValue'] * \
                                                                                                        COMCAST[
                                                                                                            'videoInstalledUnits'] + \
                                                                                                        COMCAST[
                                                                                                            'COMCAST securityValue'] * \
                                                                                                        COMCAST[
                                                                                                            'securityInstalledUnits']

#### EarthLink
## Earthlink Internet
EARTHLINK = pd.merge(EARTHLINK, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
                     right_on=['Provider-Sales', 'packageName'], how='left')
EARTHLINK = EARTHLINK[
    (EARTHLINK['orderDate'] >= EARTHLINK['start date']) & (EARTHLINK['orderDate'] <= EARTHLINK['end date']) | (
        EARTHLINK['start date'].isnull())]
EARTHLINK = EARTHLINK.rename(columns={'Commission from Provider': 'internetValue'})
EARTHLINK['internetValue'] = EARTHLINK['internetValue'].fillna(0)
del EARTHLINK['start date']
del EARTHLINK['end date']

## Earthlink Installed Internet Spiff $300 for 05/2023 - 09/2023
EARTHLINK.loc[(EARTHLINK['orderDate'] >= '2023-05-01') & (EARTHLINK['orderDate'] <= '2023-09-30') & (
            EARTHLINK['internetType'] == 'Internet'), ['internetValue']] = 300 * EARTHLINK["internetSoldUnits"]

## EARTHLINK Mobile AT $59 Each
EARTHLINK['mobileValue'] = np.where(~EARTHLINK['mobilePackage'].isnull(), 50, 0)

# Earthlink Total Value
EARTHLINK['Total Value'] = EARTHLINK['internetValue'] + EARTHLINK['mobileValue']

# Rename
EARTHLINK['Earthlink internetValue'] = EARTHLINK['internetValue']
EARTHLINK['Earthlink mobileValue'] = EARTHLINK['mobileValue']

## Combine Install date and status
### Cable Internet
EARTHLINK.loc[(~(EARTHLINK['internetInstallDate'].isnull()) & ~(EARTHLINK['internetInstallDate'] == None) & (
            EARTHLINK['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Installed_updated'

### Mobile
EARTHLINK.loc[(~(EARTHLINK['mobileInstallDate'].isnull()) & ~(EARTHLINK['mobileInstallDate'] == None) & (
            EARTHLINK['mobileOrderLineStatus'] != 'Disconnected')), ['mobileOrderLineStatus']] = 'Installed_updated'

## Combine Disconnect date and status
EARTHLINK.loc[(~(EARTHLINK['internetDisconnectDate'].isnull()) & ~(EARTHLINK['internetDisconnectDate'] == None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'
# EARTHLINK.loc[(~(EARTHLINK['wirelessInternetDisconnectDate'].isnull())&~(EARTHLINK['wirelessInternetDisconnectDate']== None)),['wirelessInternetOrderLineStatus']]='Disconnected_updated'
EARTHLINK.loc[(~(EARTHLINK['mobileDisconnectDate'].isnull()) & ~(EARTHLINK['mobileDisconnectDate'] == None)), [
    'mobileOrderLineStatus']] = 'Disconnected_updated'

### Earthlink Revenue from install
EARTHLINK['Earthlink Revenue From Install'] = 0

EARTHLINK.loc[(EARTHLINK['internetOrderLineStatus'] == 'Installed_updated') | (
            EARTHLINK['internetOrderLineStatus'] == 'Installed'), ['Earthlink Revenue From Install']] = EARTHLINK[
                                                                                                            'Earthlink Revenue From Install'] + \
                                                                                                        EARTHLINK[
                                                                                                            'internetValue']

EARTHLINK.loc[
    (EARTHLINK['mobileOrderLineStatus'] == 'Installed_updated') | (EARTHLINK['mobileOrderLineStatus'] == 'Installed'), [
        'Earthlink Revenue From Install']] = EARTHLINK['Earthlink Revenue From Install'] + EARTHLINK['mobileValue']

#### CONSOLIDATE
## CONSOLIDATE Internet
CONSOLIDATE = pd.merge(CONSOLIDATE, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
                       right_on=['Provider-Sales', 'packageName'], how='left')
CONSOLIDATE = CONSOLIDATE[
    (CONSOLIDATE['orderDate'] >= CONSOLIDATE['start date']) & (CONSOLIDATE['orderDate'] <= CONSOLIDATE['end date']) | (
        CONSOLIDATE['start date'].isnull())]
CONSOLIDATE = CONSOLIDATE.rename(columns={'Commission from Provider': 'internetValue'})
CONSOLIDATE['internetValue'] = CONSOLIDATE['internetValue'].fillna(0)
del CONSOLIDATE['start date']
del CONSOLIDATE['end date']

## CONSOLIDATE VOICE AT $35 Each
CONSOLIDATE['voiceValue'] = np.where(~CONSOLIDATE['voicePackage'].isnull(), 35, 0)

# CONSOLIDATE Total Value
CONSOLIDATE['Total Value'] = CONSOLIDATE['internetValue'] + CONSOLIDATE['voiceValue']

# Rename
CONSOLIDATE['CONSOLIDATE internetValue'] = CONSOLIDATE['internetValue']
CONSOLIDATE['CONSOLIDATE voiceValue'] = CONSOLIDATE['voiceValue']

## Combine Install date and status
### Cable Internet
CONSOLIDATE.loc[(~(CONSOLIDATE['internetInstallDate'].isnull()) & ~(CONSOLIDATE['internetInstallDate'] == None) & (
            CONSOLIDATE['internetOrderLineStatus'] != 'Disconnected')), [
    'internetOrderLineStatus']] = 'Installed_updated'

### voice
CONSOLIDATE.loc[(~(CONSOLIDATE['voiceInstallDate'].isnull()) & ~(CONSOLIDATE['voiceInstallDate'] == None) & (
            CONSOLIDATE['voiceOrderLineStatus'] != 'Disconnected')), ['voiceOrderLineStatus']] = 'Installed_updated'

## Combine Disconnect date and status
CONSOLIDATE.loc[
    (~(CONSOLIDATE['internetDisconnectDate'].isnull()) & ~(CONSOLIDATE['internetDisconnectDate'] == None)), [
        'internetOrderLineStatus']] = 'Disconnected_updated'
# CONSOLIDATE.loc[(~(CONSOLIDATE['wirelessInternetDisconnectDate'].isnull())&~(CONSOLIDATE['wirelessInternetDisconnectDate']== None)),['wirelessInternetOrderLineStatus']]='Disconnected_updated'
CONSOLIDATE.loc[(~(CONSOLIDATE['voiceDisconnectDate'].isnull()) & ~(CONSOLIDATE['voiceDisconnectDate'] == None)), [
    'voiceOrderLineStatus']] = 'Disconnected_updated'

### CONSOLIDATE Revenue from install
CONSOLIDATE['CONSOLIDATE Revenue From Install'] = 0

CONSOLIDATE.loc[(CONSOLIDATE['internetOrderLineStatus'] == 'Installed_updated') | (
            CONSOLIDATE['internetOrderLineStatus'] == 'Installed'), ['CONSOLIDATE Revenue From Install']] = CONSOLIDATE[
                                                                                                                'CONSOLIDATE Revenue From Install'] + \
                                                                                                            CONSOLIDATE[
                                                                                                                'internetValue']

CONSOLIDATE.loc[(CONSOLIDATE['voiceOrderLineStatus'] == 'Installed_updated') | (
            CONSOLIDATE['voiceOrderLineStatus'] == 'Installed'), ['CONSOLIDATE Revenue From Install']] = CONSOLIDATE[
                                                                                                             'CONSOLIDATE Revenue From Install'] + \
                                                                                                         CONSOLIDATE[
                                                                                                             'voiceValue']

# ADT
ADT['LEAD TRANSFERRED'] = 1
ADT['sale_made'] = 'true'

# ADT Security Package
ADT['packageValue'] = 51.03

# ADT Total Value
ADT['Total Value'] = ADT['packageValue']

# Rename
ADT['ADT packageValue'] = ADT['Total Value']

### ADT Revenue from install
ADT['ADT Revenue From Install'] = 0
ADT.loc[ADT['package'] == 'DIFM', 'ADT Revenue From Install'] = ADT['bundleUnit'] * 890
ADT.loc[ADT['package'] == 'DIY', 'ADT Revenue From Install'] = ADT['bundleUnit'] * 300

# CLEARCONNECT
CLEARCONNECT['LEAD TRANSFERRED'] = 1
CLEARCONNECT['sale_made'] = 'true'

# CLEARCONNECT Security Package
CLEARCONNECT['packageValue'] = 160

# CLEARCONNECT Total Value
CLEARCONNECT['Total Value'] = CLEARCONNECT['packageValue']

# Rename
CLEARCONNECT['CLEARCONNECT packageValue'] = CLEARCONNECT['Total Value']

### CLEARCONNECT Revenue from install
CLEARCONNECT['CLEARCONNECT Revenue From Install'] = 350 * CLEARCONNECT['bundleUnit']

# WOW
## WOW Internet
WOW = pd.merge(WOW, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
               right_on=['Provider-Sales', 'packageName'], how='left')
WOW = WOW[
    (WOW['orderDate'] >= WOW['start date']) & (WOW['orderDate'] <= WOW['end date']) | (WOW['start date'].isnull())]
WOW = WOW.rename(columns={'Commission from Provider': 'internetValue'})
WOW['internetValue'] = WOW['internetValue'].fillna(0)
del WOW['start date']
del WOW['end date']

## WOW Video
WOW['videoValue'] = np.where(~WOW['tvPackage'].isnull(), 60, 0)
WOW.loc[(~WOW['tvPackage'].isnull()) & (WOW['orderDate'] >= '2024-02-01'), 'videoValue'] = 25

## WOW Voice
WOW['voiceValue'] = np.where(~WOW['phonePackage'].isnull(), 75, 0)
WOW.loc[(~WOW['phonePackage'].isnull()) & (WOW['orderDate'] >= '2024-02-01'), 'voiceValue'] = 25

## WOW addon
WOW['addonValue'] = np.where(WOW['internetAddOn'].str.contains('wholehome wifi', case=False, na=False), 25, 0)
WOW.loc[(WOW['internetAddOn'].str.contains('wholehome wifi', case=False, na=False)) & (
            WOW['orderDate'] >= '2024-02-01'), 'addonValue'] = 35

# Total Value
WOW['Total Value'] = WOW['internetValue'] + WOW['videoValue'] + WOW['voiceValue'] + WOW['addonValue']
WOW['Month'] = pd.to_datetime(WOW['orderDate']).dt.month
WOW['Year'] = pd.to_datetime(WOW['orderDate']).dt.year

# Rename Revenue from sale for each product
WOW['WOW internetValue'] = WOW['internetValue']
WOW['WOW videoValue'] = WOW['videoValue']
WOW['WOW voiceValue'] = WOW['voiceValue']
WOW['WOW addonValue'] = WOW['addonValue']

## Combine Install date and status
WOW.loc[(~(WOW['internetInstallDate'].isnull()) & (WOW['internetInstallDate'] != None) & (
            WOW['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Active_updated'
WOW.loc[(~(WOW['tvInstallDate'].isnull()) & (WOW['tvInstallDate'] != None) & (
            WOW['tvOrderLineStatus'] != 'Disconnected')), ['tvOrderLineStatus']] = 'Active_updated'
WOW.loc[(~(WOW['phoneInstallDate'].isnull()) & (WOW['phoneInstallDate'] != None) & (
            WOW['phoneOrderLineStatus'] != 'Disconnected')), ['phoneOrderLineStatus']] = 'Active_updated'

## Combine Disconnect date and status
WOW.loc[(~(WOW['internetDisconnectDate'].isnull()) & (WOW['internetDisconnectDate'] != None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'
WOW.loc[(~(WOW['tvDisconnectDate'].isnull()) & (WOW['tvDisconnectDate'] != None)), [
    'tvOrderLineStatus']] = 'Disconnected_updated'
WOW.loc[(~(WOW['phoneDisconnectDate'].isnull()) & (WOW['phoneDisconnectDate'] != None)), [
    'phoneOrderLineStatus']] = 'Disconnected_updated'

# WOW Chargeback
WOW['WOW Chargeback'] = 0
WOW.loc[
    (WOW['internetOrderLineStatus'] == 'Disconnected_updated') | (WOW['internetOrderLineStatus'] == 'Disconnected'), [
        'WOW Chargeback']] = WOW['WOW Chargeback'] + WOW['internetValue']
WOW.loc[(WOW['tvOrderLineStatus'] == 'Disconnected_updated') | (WOW['tvOrderLineStatus'] == 'Disconnected'), [
    'WOW Chargeback']] = WOW['WOW Chargeback'] + WOW['videoValue']
WOW.loc[(WOW['phoneOrderLineStatus'] == 'Disconnected_updated') | (WOW['phoneOrderLineStatus'] == 'Disconnected'), [
    'WOW Chargeback']] = WOW['WOW Chargeback'] + WOW['voiceValue']

# WOW Disconnect Unit
WOW['WOW Disconnect Unit'] = 0
WOW.loc[
    (WOW['internetOrderLineStatus'] == 'Disconnected_updated') | (WOW['internetOrderLineStatus'] == 'Disconnected'), [
        'WOW Disconnect Unit']] = WOW['WOW Disconnect Unit'] + 1
WOW.loc[(WOW['tvOrderLineStatus'] == 'Disconnected_updated') | (WOW['tvOrderLineStatus'] == 'Disconnected'), [
    'WOW Disconnect Unit']] = WOW['WOW Disconnect Unit'] + 1
WOW.loc[(WOW['phoneOrderLineStatus'] == 'Disconnected_updated') | (WOW['phoneOrderLineStatus'] == 'Disconnected'), [
    'WOW Disconnect Unit']] = WOW['WOW Disconnect Unit'] + 1

### WOW Revenue from install
WOW['WOW Revenue From Install'] = 0
WOW.loc[(WOW['internetOrderLineStatus'] == 'Active_updated') | (
    WOW['internetOrderLineStatus'].isin(['Active', 'Installed'])), ['WOW Revenue From Install']] = WOW[
                                                                                                       'WOW Revenue From Install'] + \
                                                                                                   WOW['internetValue']
WOW.loc[(WOW['tvOrderLineStatus'] == 'Active_updated') | (WOW['tvOrderLineStatus'].isin(['Active', 'Installed'])), [
    'WOW Revenue From Install']] = WOW['WOW Revenue From Install'] + WOW['videoValue']
WOW.loc[
    (WOW['phoneOrderLineStatus'] == 'Active_updated') | (WOW['phoneOrderLineStatus'].isin(['Active', 'Installed'])), [
        'WOW Revenue From Install']] = WOW['WOW Revenue From Install'] + WOW['voiceValue']

##COX
## COX Internet
COX = pd.merge(COX, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
               right_on=['Provider-Sales', 'packageName'], how='left')
COX = COX[
    (pd.to_datetime(COX['orderDate']) >= COX['start date']) & (pd.to_datetime(COX['orderDate']) <= COX['end date']) | (
        COX['start date'].isnull())]
COX = COX.rename(columns={'Commission from Provider': 'internetValue'})
COX['internetValue'] = COX['internetValue'].fillna(0)
del COX['start date']
del COX['end date']

## COX Video

COX = pd.merge(COX, Video_table, left_on=['Provider-Sales', 'videoPackage'], right_on=['Provider-Sales', 'packageName'],
               how='left')
COX = COX[
    (pd.to_datetime(COX['orderDate']) >= COX['start date']) & (pd.to_datetime(COX['orderDate']) <= COX['end date']) | (
        COX['start date'].isnull())]
COX = COX.rename(columns={'Commission from Provider': 'videoValue'})
COX['videoValue'] = COX['videoValue'].fillna(0)
del COX['start date']
del COX['end date']

# COX['videoValue'] = np.where(~COX['videoPackage'].isnull(),0)
# COX.loc[(~COX['videoPackage'].isnull()) & (COX['orderDate']>= '2024-02-01'), 'videoValue' ]

## COX VOICE

COX = pd.merge(COX, Voice_table, left_on=['Provider-Sales', 'voicePackage'], right_on=['Provider-Sales', 'packageName'],
               how='left')
COX = COX[
    (pd.to_datetime(COX['orderDate']) >= COX['start date']) & (pd.to_datetime(COX['orderDate']) <= COX['end date']) | (
        COX['start date'].isnull())]
COX = COX.rename(columns={'Commission from Provider': 'voiceValue'})
COX['voiceValue'] = COX['voiceValue'].fillna(0)
del COX['start date']
del COX['end date']

# COX['voiceValue'] = np.where(~COX['voicePackage'].isnull(),50,0)
# COX.loc[(~COX['voicePackage'].isnull()) & (COX['orderDate']>= '2024-02-01'), 'voiceValue' ] = 25


# Total Value
COX['Total Value'] = COX['internetValue'] + COX['videoValue'] + COX['voiceValue']

# Rename Revenue from sale for each product
COX['COX internetValue'] = COX['internetValue']
COX['COX videoValue'] = COX['videoValue']
COX['COX voiceValue'] = COX['voiceValue']

## Combine Install date and status
COX.loc[(~(COX['internetInstallDate'].isnull()) & (COX['internetInstallDate'] != None) & (
            COX['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Active_updated'
COX.loc[(~(COX['videoInstallDate'].isnull()) & (COX['videoInstallDate'] != None) & (
            COX['videoOrderLineStatus'] != 'Disconnected')), ['videoOrderLineStatus']] = 'Active_updated'
COX.loc[(~(COX['voiceInstallDate'].isnull()) & (COX['voiceInstallDate'] != None) & (
            COX['voiceOrderLineStatus'] != 'Disconnected')), ['voiceOrderLineStatus']] = 'Active_updated'

## Combine Disconnect date and status
COX.loc[(~(COX['internetDisconnectDate'].isnull()) & (COX['internetDisconnectDate'] != None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'
COX.loc[(~(COX['videoDisconnectDate'].isnull()) & (COX['videoDisconnectDate'] != None)), [
    'videoOrderLineStatus']] = 'Disconnected_updated'
COX.loc[(~(COX['voiceDisconnectDate'].isnull()) & (COX['voiceDisconnectDate'] != None)), [
    'voiceOrderLineStatus']] = 'Disconnected_updated'

### COX Chargeback
COX['COX Chargeback'] = 0
COX.loc[
    (COX['internetOrderLineStatus'] == 'Disconnected_updated') | (COX['internetOrderLineStatus'] == 'Disconnected'), [
        'COX Chargeback']] = COX['COX Chargeback'] + COX['internetValue']
COX.loc[(COX['videoOrderLineStatus'] == 'Disconnected_updated') | (COX['videoOrderLineStatus'] == 'Disconnected'), [
    'COX Chargeback']] = COX['COX Chargeback'] + COX['videoValue']
COX.loc[(COX['voiceOrderLineStatus'] == 'Disconnected_updated') | (COX['voiceOrderLineStatus'] == 'Disconnected'), [
    'COX Chargeback']] = COX['COX Chargeback'] + COX['voiceValue']

# COX Disconnect Unit
COX['COX Disconnect Unit'] = 0
COX.loc[
    (COX['internetOrderLineStatus'] == 'Disconnected_updated') | (COX['internetOrderLineStatus'] == 'Disconnected'), [
        'COX Disconnect Unit']] = COX['COX Disconnect Unit'] + 1
COX.loc[(COX['videoOrderLineStatus'] == 'Disconnected_updated') | (COX['videoOrderLineStatus'] == 'Disconnected'), [
    'COX Disconnect Unit']] = COX['COX Disconnect Unit'] + 1
COX.loc[(COX['voiceOrderLineStatus'] == 'Disconnected_updated') | (COX['voiceOrderLineStatus'] == 'Disconnected'), [
    'COX Disconnect Unit']] = COX['COX Disconnect Unit'] + 1

### COX Revenue from install
COX['COX Revenue From Install'] = 0
COX.loc[(COX['internetOrderLineStatus'] == 'Active_updated') | (
    COX['internetOrderLineStatus'].isin(['Active', 'Installed'])), ['COX Revenue From Install']] = COX[
                                                                                                       'COX Revenue From Install'] + \
                                                                                                   COX['internetValue']
COX.loc[
    (COX['videoOrderLineStatus'] == 'Active_updated') | (COX['videoOrderLineStatus'].isin(['Active', 'Installed'])), [
        'COX Revenue From Install']] = COX['COX Revenue From Install'] + COX['videoValue']
COX.loc[
    (COX['voiceOrderLineStatus'] == 'Active_updated') | (COX['voiceOrderLineStatus'].isin(['Active', 'Installed'])), [
        'COX Revenue From Install']] = COX['COX Revenue From Install'] + COX['voiceValue']

##TMOBILE
## TMOBILE Internet
TMOBILE = pd.merge(TMOBILE, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
                   right_on=['Provider-Sales', 'packageName'], how='left')
TMOBILE = TMOBILE[(pd.to_datetime(TMOBILE['orderDate']) >= TMOBILE['start date']) & (
            pd.to_datetime(TMOBILE['orderDate']) <= TMOBILE['end date']) | (TMOBILE['start date'].isnull())]
TMOBILE = TMOBILE.rename(columns={'Commission from Provider': 'internetValue'})
TMOBILE['internetValue'] = TMOBILE['internetValue'].fillna(0)
del TMOBILE['start date']
del TMOBILE['end date']

# Total Value
TMOBILE['Total Value'] = TMOBILE['internetValue']

# Rename Revenue from sale for each product
TMOBILE['TMOBILE internetValue'] = TMOBILE['internetValue']

## Combine Install date and status
TMOBILE.loc[(~(TMOBILE['internetInstallDate'].isnull()) & (TMOBILE['internetInstallDate'] != None) & (
            TMOBILE['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Active_updated'

## Combine Disconnect date and status
TMOBILE.loc[(~(TMOBILE['internetDisconnectDate'].isnull()) & (TMOBILE['internetDisconnectDate'] != None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'

### TMOBILE Chargeback
TMOBILE['TMOBILE Chargeback'] = 0
TMOBILE.loc[(TMOBILE['internetOrderLineStatus'] == 'Disconnected_updated') | (
            TMOBILE['internetOrderLineStatus'] == 'Disconnected'), ['TMOBILE Chargeback']] = TMOBILE[
                                                                                                 'TMOBILE Chargeback'] + \
                                                                                             TMOBILE['internetValue']

# TMOBILE Disconnect Unit
TMOBILE['TMOBILE Disconnect Unit'] = 0
TMOBILE.loc[(TMOBILE['internetOrderLineStatus'] == 'Disconnected_updated') | (
            TMOBILE['internetOrderLineStatus'] == 'Disconnected'), ['TMOBILE Disconnect Unit']] = TMOBILE[
                                                                                                      'TMOBILE Disconnect Unit'] + 1

### TMOBILE Revenue from install
TMOBILE['TMOBILE Revenue From Install'] = 0
TMOBILE.loc[(TMOBILE['internetOrderLineStatus'] == 'Active_updated') | (
    TMOBILE['internetOrderLineStatus'].isin(['Active', 'Installed'])), ['TMOBILE Revenue From Install']] = TMOBILE[
                                                                                                               'TMOBILE Revenue From Install'] + \
                                                                                                           TMOBILE[
                                                                                                               'internetValue']

##WINDSTREAM
## WS Internet
WS = pd.merge(WS, Internet_table, left_on=['Provider-Sales', 'internetPackage'],
              right_on=['Provider-Sales', 'packageName'], how='left')
WS = WS[(pd.to_datetime(WS['orderDate']) >= WS['start date']) & (pd.to_datetime(WS['orderDate']) <= WS['end date']) | (
    WS['start date'].isnull())]
WS = WS.rename(columns={'Commission from Provider': 'internetValue'})
WS['internetValue'] = WS['internetValue'].fillna(0)
del WS['start date']
del WS['end date']

## WS VOICE

WS = pd.merge(WS, Voice_table, left_on=['Provider-Sales', 'voicePackage'], right_on=['Provider-Sales', 'packageName'],
              how='left')
WS = WS[(pd.to_datetime(WS['orderDate']) >= WS['start date']) & (pd.to_datetime(WS['orderDate']) <= WS['end date']) | (
    WS['start date'].isnull())]
WS = WS.rename(columns={'Commission from Provider': 'voiceValue'})
WS['voiceValue'] = WS['voiceValue'].fillna(0)
del WS['start date']
del WS['end date']

# WS['voiceValue'] = np.where(~WS['voicePackage'].isnull(),75,0)
# WS.loc[(~WS['voicePackage'].isnull()) & (WS['orderDate']>= '2024-07-01'), 'voiceValue' ] = 25


# Total Value
WS['Total Value'] = WS['internetValue'] + WS['voiceValue']

# Rename Revenue from sale for each product
WS['WS internetValue'] = WS['internetValue']
WS['WS voiceValue'] = WS['voiceValue']

## Combine Install date and status
WS.loc[(~(WS['internetInstallDate'].isnull()) & (WS['internetInstallDate'] != None) & (
            WS['internetOrderLineStatus'] != 'Disconnected')), ['internetOrderLineStatus']] = 'Active_updated'
WS.loc[(~(WS['voiceInstallDate'].isnull()) & (WS['voiceInstallDate'] != None) & (
            WS['voiceOrderLineStatus'] != 'Disconnected')), ['voiceOrderLineStatus']] = 'Active_updated'

## Combine Disconnect date and status
WS.loc[(~(WS['internetDisconnectDate'].isnull()) & (WS['internetDisconnectDate'] != None)), [
    'internetOrderLineStatus']] = 'Disconnected_updated'
WS.loc[(~(WS['voiceDisconnectDate'].isnull()) & (WS['voiceDisconnectDate'] != None)), [
    'voiceOrderLineStatus']] = 'Disconnected_updated'

### WS Chargeback
WS['WS Chargeback'] = 0
WS.loc[(WS['internetOrderLineStatus'] == 'Disconnected_updated') | (WS['internetOrderLineStatus'] == 'Disconnected'), [
    'WS Chargeback']] = WS['WS Chargeback'] + WS['internetValue']
WS.loc[(WS['voiceOrderLineStatus'] == 'Disconnected_updated') | (WS['voiceOrderLineStatus'] == 'Disconnected'), [
    'WS Chargeback']] = WS['WS Chargeback'] + WS['voiceValue']

# WS Disconnect Unit
WS['WS Disconnect Unit'] = 0
WS.loc[(WS['internetOrderLineStatus'] == 'Disconnected_updated') | (WS['internetOrderLineStatus'] == 'Disconnected'), [
    'WS Disconnect Unit']] = WS['WS Disconnect Unit'] + 1
WS.loc[(WS['voiceOrderLineStatus'] == 'Disconnected_updated') | (WS['voiceOrderLineStatus'] == 'Disconnected'), [
    'WS Disconnect Unit']] = WS['WS Disconnect Unit'] + 1

### WS Revenue from install
WS['WS Revenue From Install'] = 0
WS.loc[(WS['internetOrderLineStatus'] == 'Active_updated') | (
    WS['internetOrderLineStatus'].isin(['Active', 'Installed'])), ['WS Revenue From Install']] = WS[
                                                                                                     'WS Revenue From Install'] + \
                                                                                                 WS['internetValue']
WS.loc[(WS['voiceOrderLineStatus'] == 'Active_updated') | (WS['voiceOrderLineStatus'].isin(['Active', 'Installed'])), [
    'WS Revenue From Install']] = WS['WS Revenue From Install'] + WS['voiceValue']

Sale = pd.concat(
    [SPC, ATT, OPT_SUD, Viasat, Frontier, Vivint, Alder, COMCAST, EARTHLINK, DTV, CONSOLIDATE, CLEARCONNECT, ADT, WOW,
     COX, TMOBILE, WS]).reset_index()
Sale = Sale.drop_duplicates()
untrack = Sale.copy()
Sale = Sale[(Sale['sale_made'].str.contains("true", na=False, case=False)) & (Sale['Total Value'] > 0)].reset_index()
# Sale['space removed'] = Sale['campaignName'].apply(correct_names)
Sale['space removed'] = Sale['campaignName'].apply(
    lambda x: str(x).replace(' ', '').replace(',', '').replace('-', '').replace(':', '').replace('(', '').replace(')',
                                                                                                                  '').lower())
Sale = Sale.merge(relation2, left_on=["space removed"], right_on=["portal space removed"], how="left")
Sale = Sale.merge(relation4, on=["UID"], how="left")
Sale = Sale[~Sale['UID'].isnull()]
Sale = Sale.drop_duplicates()

Sale['SPC Revenue'] = 0
Sale['ATT Revenue'] = 0
Sale['OPM Revenue'] = 0
Sale['VIASAT Revenue'] = 0
Sale['FRONTIER Revenue'] = 0
Sale['VIVINT Revenue'] = 0
Sale['ALDER Revenue'] = 0
Sale['COMCAST Revenue'] = 0
Sale['Earthlink Revenue'] = 0
Sale['DTV Revenue'] = 0
Sale['CONSOLIDATE Revenue'] = 0
Sale['ADT Revenue'] = 0
Sale['CLEARCONNECT Revenue'] = 0
Sale['WOW Revenue'] = 0
Sale['COX Revenue'] = 0
Sale['TMOBILE Revenue'] = 0
Sale['WS Revenue'] = 0

Sale.loc[Sale['Provider-Sales'] == 'SPC', ['SPC Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'ATT', ['ATT Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'OPM', ['OPM Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'VIASAT', ['VIASAT Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'FRONTIER', ['FRONTIER Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'VIVINT', ['VIVINT Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'ALDER', ['ALDER Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'COMCAST', ['COMCAST Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'EARTHLINK', ['Earthlink Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'DTV', ['DTV Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'CONSOLIDATE', ['CONSOLIDATE Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'CLEARCONNECT', ['CLEARCONNECT Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'ADT', ['ADT Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'WOW', ['WOW Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'COX', ['COX Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'TMOBILE', ['TMOBILE Revenue']] = Sale['Total Value']
Sale.loc[Sale['Provider-Sales'] == 'WS', ['WS Revenue']] = Sale['Total Value']
print(Sale['Provider-Sales'] == 'COX')
# Add in DAtetime

# write your script here
Sale = Sale.rename(columns={"MobileLines": "mobileLines"})

# Generate Datetime In Sale DB
Sale['TimeFrame'] = Sale['TimeFrame'].str.replace('1970-01-01', '')
Sale['Datetime'] = pd.to_datetime(Sale['orderDate']).dt.date.astype(str) + ' ' + Sale['TimeFrame']
Sale['Datetime'] = pd.to_datetime(Sale['Datetime'])
from datetime import datetime
from pytz import timezone
import pytz

Sale['Datetime'] = Sale['Datetime'].dt.tz_localize('US/Pacific').dt.tz_convert('UTC')
Sale['Datetime'] = Sale['Datetime'].map(lambda x: x.replace(tzinfo=None))

Sale_gb = Sale.groupby(['Datetime', 'Portal Camp', 'retailerName']).agg({'ATT Revenue From Install': 'sum',
                                                                         'DTV Revenue From Install': 'sum',
                                                                         'SPC Revenue From Install': 'sum',
                                                                         'OPT_SUD Revenue From Install': 'sum',
                                                                         'Viasat Revenue From Install': 'sum',
                                                                         'Frontier Revenue From Install': 'sum',
                                                                         'Vivint Revenue From Install': 'sum',
                                                                         'Alder Revenue From Install': 'sum',
                                                                         'ALDER Revenue': 'sum',
                                                                         'SPC Revenue': 'sum',
                                                                         'ATT Revenue': 'sum',
                                                                         'DTV Revenue': 'sum',
                                                                         'OPM Revenue': 'sum',
                                                                         'VIASAT Revenue': 'sum',
                                                                         'FRONTIER Revenue': 'sum',
                                                                         'VIVINT Revenue': 'sum',
                                                                         'COMCAST Revenue': 'sum',
                                                                         'COMCAST Revenue From Install': 'sum',
                                                                         'Earthlink Revenue': 'sum',
                                                                         'Earthlink Revenue From Install': 'sum',
                                                                         'CONSOLIDATE Revenue': 'sum',
                                                                         'CONSOLIDATE Revenue From Install': 'sum',
                                                                         'ADT Revenue': 'sum',
                                                                         'ADT Revenue From Install': 'sum',
                                                                         'ATT internetValue': 'sum',
                                                                         'ATT videoValue': 'sum',
                                                                         'ATT voiceValue': 'sum',
                                                                         'ATT addonValue': 'sum',
                                                                         'ATT wirelessValue': 'sum',
                                                                         'DTV videoValue': 'sum',
                                                                         'DTV addonValue': 'sum',

                                                                         'SPC internetValue': 'sum',
                                                                         'SPC videoValue': 'sum',
                                                                         'SPC phoneValue': 'sum',
                                                                         'SPC mobileValue': 'sum',
                                                                         'ALTICE internetValue': 'sum',
                                                                         'ALTICE videoValue': 'sum',
                                                                         'ALTICE phoneValue': 'sum',
                                                                         'ALTICE addonValue': 'sum',
                                                                         'Viasat internetValue': 'sum',
                                                                         'Viasat phoneValue': 'sum',
                                                                         'Viasat addonsValue': 'sum',
                                                                         'Frontier internetValue': 'sum',
                                                                         'Frontier voiceValue': 'sum',
                                                                         'Vivint packageValue': 'sum',
                                                                         'Alder packageValue': 'sum',
                                                                         'COMCAST internetValue': 'sum',
                                                                         'COMCAST voiceValue': 'sum',
                                                                         'COMCAST videoValue': 'sum',
                                                                         'COMCAST securityValue': 'sum',
                                                                         'Earthlink internetValue': 'sum',
                                                                         'Earthlink mobileValue': 'sum',
                                                                         'CONSOLIDATE internetValue': 'sum',
                                                                         'CONSOLIDATE voiceValue': 'sum',
                                                                         'CLEARCONNECT packageValue': 'sum',
                                                                         'ADT packageValue': 'sum',
                                                                         'WOW Revenue From Install': 'sum',
                                                                         'WOW Revenue': 'sum',
                                                                         'WOW internetValue': 'sum',
                                                                         'WOW videoValue': 'sum',
                                                                         'WOW voiceValue': 'sum',
                                                                         'WOW addonValue': 'sum', 'Total Value': 'sum',
                                                                         'Frontier wirelessValue': 'sum',
                                                                         'ALTICE mobileValue': 'sum',
                                                                         'COX Revenue From Install': 'sum',
                                                                         'COX Revenue': 'sum',
                                                                         'COX internetValue': 'sum',
                                                                         'COX videoValue': 'sum',
                                                                         'COX voiceValue': 'sum',
                                                                         'TMOBILE Revenue From Install': 'sum',
                                                                         'TMOBILE Revenue': 'sum',
                                                                         'TMOBILE internetValue': 'sum',
                                                                         'WS Revenue From Install': 'sum',
                                                                         'WS Revenue': 'sum',
                                                                         'WS internetValue': 'sum',
                                                                         'WS voiceValue': 'sum'
                                                                         }).reset_index()
Sale_gb.loc[Sale_gb['retailerName'] == 'TPP Call Center X', ['retailerName']] = 'PK'
Sale_gb.loc[Sale_gb['retailerName'] == 'XYZies - Call Center EX', ['retailerName']] = 'COL'
# Sale_gb['Month'] = pd.to_datetime(Sale_gb['orderDate']).dt.month
# Sale_gb['Year'] = pd.to_datetime(Sale_gb['orderDate']).dt.year


Sale_gb = Sale_gb.fillna(0)
# Sale_gb['orderDate'] = Sale_gb['orderDate'].dt.date

# write a data frame so it's available to the next action
write_dataframe(Sale_gb)