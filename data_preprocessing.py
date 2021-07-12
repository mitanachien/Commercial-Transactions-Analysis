# Created by Mitana Chien
# Data Preprocessing

import pandas as pd
import json
import time

# load data using Python JSON module
with open('brands.json', 'r', encoding="utf-8") as f:
  brands_data = [json.loads(line) for line in f]
with open('receipts.json', 'r', encoding="utf-8") as f:
  receipts_data = [json.loads(line) for line in f]
with open('users.json', 'r', encoding="utf-8") as f:
  users_data = [json.loads(line) for line in f]

# Brands data parsing
brands_len = len(brands_data)
new_brands_data = [{} for i in range(brands_len)]

for dic in range(brands_len):
  for key in brands_data[dic].keys():
    if key == '_id':
      new_brands_data[dic]['id'] = brands_data[dic][key]['$oid']
    elif key == 'cpg':
      new_brands_data[dic]['cpg_id'] = brands_data[dic][key]['$id']['$oid']
      new_brands_data[dic]['cpg_ref'] = brands_data[dic][key]['$ref']
    else:
      new_brands_data[dic][key] = brands_data[dic][key]

brands_df = pd.DataFrame(new_brands_data)

# Users data parsing
# Convert the unix epoch time to readable date
users_len = len(users_data)
new_users_data = [{} for i in range(users_len)]

for dic in range(users_len):
  for key in users_data[dic].keys():
    if key == '_id':
      new_users_data[dic]['id'] = users_data[dic][key]['$oid']
    elif key == 'createdDate' or key == 'lastLogin':
      new_users_data[dic][key] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(users_data[dic][key]['$date']/1000))
    else:
      new_users_data[dic][key] = users_data[dic][key]

users_df = pd.DataFrame(new_users_data)

# Receipts data parsing
# Create another table for rewards receipt item list
receipts_len = len(receipts_data)
new_receipts_data = [{} for i in range(receipts_len)]
receipt_items_data = [{} for i in range(6941)]
row = 0

for dic in range(receipts_len):
  for key in receipts_data[dic].keys():
    if key == '_id':
      new_receipts_data[dic]['id'] = receipts_data[dic][key]['$oid']
    elif key in ['createDate', 'dateScanned', 'modifyDate', 'pointsAwardedDate', 'purchaseDate', 'finishedDate']:
      new_receipts_data[dic][key] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(receipts_data[dic][key]['$date']/1000))
    elif key == 'rewardsReceiptItemList':
      for reward in range(len(receipts_data[dic][key])):
        receipt_items_data[row]['receiptId'] = receipts_data[dic]['_id']['$oid']
        for reward_key in receipts_data[dic][key][reward].keys():
          receipt_items_data[row][reward_key] = receipts_data[dic][key][reward][reward_key]
        row += 1    
    else:
      new_receipts_data[dic][key] = receipts_data[dic][key]

receipts_df = pd.DataFrame(new_receipts_data)

receipt_items_df = pd.DataFrame(receipt_items_data)

# Save dataframes to csv files
brands_df.to_csv('brands.csv', index=False)
users_df.to_csv('users.csv', index=False)
receipts_df.to_csv('receipts.csv', index=False)
receipt_items_df.to_csv('receipt_items.csv', index=False)
