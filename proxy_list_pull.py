import pandas as pd
import numpy as np
import datetime
import time
import re
import os
import pypyodbc
import warnings

warnings.filterwarnings('ignore')
timestamp = (datetime.datetime.today() + datetime.timedelta(1)).strftime('%m-%d-%y %H-%M')
#tmrw = pd.to_datetime('today') + datetime.timedelta(1)
t = datetime.date.fromtimestamp(time.time())
tmrw = pd.to_datetime(t) + datetime.timedelta(1)
rng = pd.date_range(start=tmrw, periods=24, freq='H') #range of hours of tomorrow
tmrw_str = (datetime.datetime.today() + datetime.timedelta(1)).strftime('%m-%d-%y')

dir_R = r'R:\Carvana\Merchandising'
dir_buyList = r'\\sp-att-dsapp-01\vauto_uploads\BuyListSpreadsheets'
dir_autoList = r'\\sp-att-dsapp-01\vauto_uploads\BuyListSpreadsheets\Daily Proxy\Automated Proxy List'
dir_home = r'C:\Users\sguy'

#Create connection
engine = pypyodbc.connect(r'Driver={SQL Server};Server=dwhcarprod\datawarehousecar;Database=Analytics;Trusted_Connection=yes;')

# import source data from SQL Server
stp = "execute Analytics.[dbo].[stpSelectPurchasingBuyerReadModel_Carlypso_v5]"
data = pd.read_sql_query(stp, engine, parse_dates=['auctionstartdate','auctionenddate','createddatetime'])
engine.close()

# transportation excel sheet
os.chdir(dir_buyList)
transheet = pd.read_excel('transport_sheet.xlsx')
transheet['name'] = transheet['Auction Name'].str.lower().str.rstrip().str.lstrip()
os.chdir(dir_home)

# INITIAL FILTER SETUP
data = data.query('ready_to_buy == 1')
source_list = ['AdesaRunListLiveBlock','Manheim InLane','Manheim Preview','Manheim Unknown', 'AdesaRunList']

# columns used
select_columns = ['buyer_individual', 'source', 'auctionstartdate', 'auctionenddate',
       'buyerclassification', 'salelocation', 'iclocation', 'vin',
       'carvana_comments', 'year', 'make', 'model', 'trim',
       'rbook_perc_proper', 'required_margin', 'excess_margin', 'recon_cost',
       'transport_cost', 'mileage', 'crgrade', 'runnumber', 'auctionlane',
       'seller', 'hasnocriticalrecalls', 'buyerfee', 'cosmeticcost',
       'mechanicalcost', 'tire', 'tirecost', 'truetirecost', 'stickerprice',
       'model_auction_price_estimate', 'kbbvalue', 'maxbid', 'mmrprice',
       'current_auction_price', 'condition', 'cum_cdf_val', 'prob_ladder',
       'requiredmarginadjustment', 'distance', 'from_zipcode', 'to_zipcode',
       'listing_type', 'createddatetime', 'mktcheck_confidence_score',
       'mmrprice', 'ready_to_buy', 'secondary_source_with_end_datetime',
       'forward_supply_30', 'forward_supply_60',
       'sticker_by_rbook_60_factor_mm', 'sticker_by_rbook_60_units_mm',
       'sticker_by_rbook_60_factor_mmt', 'sticker_by_rbook_60_units_mmt',
       'k_cvna', 'exterior_color', 'safe_maxbid', 'ok_to_autobid',
       'condition_report_process_date', 'lowest_known_cr_rating', 'mmrratio',
       'transmission_type', 'drivetrain_type', 'rbook_override',
       'rbook_inv_counts', 'rbook_inv_percent', 'rbook_odo_adjusted_value', 'url', 'adjusted_cycle_time',
       'uid']
data = data[[col for col in data.columns if col in select_columns]]
data = data.query('crgrade == "NaN" | crgrade >= 3')

# PRIMARY SOURCE DATA
primary = data[data.source.isin(source_list)]
primary['name'] = primary.salelocation.str.lower().str.rstrip()
primary['name'] = primary['name'].apply(lambda x: re.sub('^.*?(?=-).', '', str(x))).str.lstrip()
primary = primary.reset_index(drop=True)
primary = primary[(primary['auctionenddate']>=rng[0]) & (primary['auctionenddate']<=rng[-1])]#filter on tomorrows data
primary = primary.query('secondary_source_with_end_datetime == "None"')

# SECONDARY SOURCE DATA
secondary = data.query('secondary_source_with_end_datetime != "None"')
secondary = secondary.query('ready_to_buy == 1')
secondary['name'] = secondary.salelocation.str.lower()


#functions
def split_clean (df, column, sep):
    splits = df[column].str.split(sep, expand=True)
    splits[0] = splits[0].str.rstrip()
    return splits

def clean_name(frame):
    if len(str(frame[0]))>3:
        return str(frame[0]).lower()
    else:
        return str(frame[1]).lower()

def bclean(s):
    s = s[2:]
    s = s[:-1]
    return s

# secondary source transformations and filtering
split = secondary.secondary_source_with_end_datetime.str.split(",",n=3,expand=True)

split[2] = pd.to_datetime(split[2])
split = split[(split[2]>=rng[0]) & (split[2]<=rng[-1])]

split[1] = split[1].apply(lambda x: re.sub('^.*?(?=-).', "", x)).str.lower().str.lstrip()
secondary_clean = secondary[secondary.index.isin(split.index)]
secondary_clean['source'] = split[0]
secondary_clean['auctionstartdate'] = split[2]
secondary_clean['auctionenddate'] = split[2]
secondary_clean['name'] = split[1]
secondary_clean['runnumber'] = split[3].str.split(":",expand=True)[1].str.split(";",expand=True)[0]

#convert timezone
secondary_clean['auctionstartdate'] = secondary_clean.auctionstartdate.dt.tz_localize('UTC').dt.tz_convert('MST')
secondary_clean['auctionstartdate'] = secondary_clean.auctionstartdate.dt.tz_localize(None)

secondary_clean['auctionenddate'] = secondary_clean.auctionstartdate.dt.tz_localize('UTC').dt.tz_convert('MST')
secondary_clean['auctionenddate'] = secondary_clean.auctionstartdate.dt.tz_localize(None)

# MAIN DATA
main = primary.append(secondary_clean)
main = main.merge(transheet[['name','Destination IC','Transport Cost']], on='name', how='inner')
main['iclocation'] = main['Destination IC']
del main['Destination IC']
main = main.query('name != "none"')
main['uid'] = main['uid'].apply(bclean)

# proxy list to export to excel
proxy_list = main.drop_duplicates(subset='vin')
proxy_list['tire'] = proxy_list['required_margin'] / proxy_list['stickerprice']
proxy_list_size = proxy_list.index.size
os.chdir(dir_autoList)
proxy_list.to_excel('proxy_list_'+tmrw_str+'_main.xlsx', sheet_name='Proxy List', index=False)


