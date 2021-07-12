# Created by Mitana Chien
# Data Cleaning

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Read data
brands_df = pd.read_csv('brands.csv')
receipts_df = pd.read_csv('receipts.csv')
receipt_items_df = pd.read_csv('receipt_items.csv')
users_df = pd.read_csv('users.csv')

# Clean brands table
brands_df.info()

# Check duplicated brands data
# Check missing values in each column
# Confirm id can be the primary key of brands
print("Duplicated values in id column : \n" , brands_df.duplicated(subset=['id']).sum())
print("Missing values in every column : \n" ,brands_df.isnull().sum())

# Select essential fields
selected_brands_df = brands_df[['id', 'barcode', 'name', 'brandCode']]

# Clean receipts table
receipts_df.info()

# Check duplicated receipts data
# Check missing values in each column
# Confirm id can be the primary key of users
print("Duplicated values in id column : \n" , receipts_df.duplicated(subset=['id']).sum())
print("Missing values in every column : \n" , receipts_df.isnull().sum())

# Select essential fields
selected_receipts_df = receipts_df[['id', 'dateScanned', 'purchasedItemCount', 'rewardsReceiptStatus', 'totalSpent', 'userId']]

# Draw a box plot to detect if there are outliers
selected_receipts_df.plot(kind = 'box', subplots = True, layout=(4, 3), sharex = False, sharey= False, figsize=(20, 20))
plt.show()

# Check if it makes sense that the more purchased items count, the more the total spent.
# To make sure the outliers are not caused by some errors.
selected_receipts_df[selected_receipts_df['purchasedItemCount']>300]

# Clean receipt items table
receipt_items_df.info()

# Check duplicated receipt items data
print("Duplicated values in receiptId column : \n" , receipt_items_df.duplicated(subset=['receiptId']).sum())
print("Duplicated values in every columns : \n" , receipt_items_df.duplicated().sum())

# Select essential fields
selected_receipt_items_df = receipt_items_df[['receiptId', 'barcode', 'userFlaggedBarcode', 'brandCode']]

"""There is a data quality issue of incomplete information about the 'barcode' and 'userFlaggedBarcode' columns. The two columns seem to contain the same kind of information but separate in different fields, so I merged the data of these two columns based on taking the 'barcode' column as the main information. After that, I dropped missing rows that contained nulls in both 'barcode' and 'brandCode' columns. And fill a string 'missing' in the blank cell to group them successfully.
There is another data quality issue of the inconsistent format in the 'barcode' column. The column contains string type and float type at the same time. After removing data with value = 4011, I thought the datatype may be all strings.
"""

# Combine infomation in 'barcode' and 'userFlaggedBarcode' and drop missing data
len_ = len(selected_receipt_items_df)
for row in range(len_):
  if pd.isna(selected_receipt_items_df.loc[row, 'barcode']) and not pd.isna(selected_receipt_items_df.loc[row, 'userFlaggedBarcode']):
    selected_receipt_items_df.loc[row, 'barcode'] = selected_receipt_items_df.loc[row, 'userFlaggedBarcode']
  if (selected_receipt_items_df.loc[row, 'barcode'] == '4011' or selected_receipt_items_df.loc[row, 'barcode'] == 4011) and selected_receipt_items_df.loc[row, 'userFlaggedBarcode'] != 4011\
  and not pd.isna(selected_receipt_items_df.loc[row, 'userFlaggedBarcode']):
    selected_receipt_items_df.loc[row, 'barcode'] = selected_receipt_items_df.loc[row, 'userFlaggedBarcode']
  if (pd.isna(selected_receipt_items_df.loc[row, 'barcode']) or selected_receipt_items_df.loc[row, 'barcode'] == '4011' or selected_receipt_items_df.loc[row, 'barcode'] == 4011) and pd.isna(selected_receipt_items_df.loc[row, 'brandCode']):
    selected_receipt_items_df.drop(row, inplace=True)

# Select essential fields
selected_receipt_items_df = selected_receipt_items_df[['receiptId', 'barcode', 'brandCode']]

# Replace missing value as 'missing'
selected_receipt_items_df.fillna('missing', inplace=True)

# Check duplicated receipt items data
print("Duplicated values in specific columns : \n" , selected_receipt_items_df.duplicated().sum())

# Group the fields and count the number of duplicated rows (This means how many same items were purchased on the receipt)
group_receipt_items_df = selected_receipt_items_df.groupby(['receiptId', 'barcode', 'brandCode']).size().reset_index(name='counts')

# Clean users table
users_df.info()

# Check duplicated users data
print("Duplicated values in id columns : \n" , users_df.duplicated(subset=['id']).sum())
print("Duplicated values in specific columns : \n" , users_df.duplicated(subset=['id', 'createdDate']).sum())

"""There is a data quality issue about duplicated data in the users data. There are multiple rows contain the same 'id' and 'createdDate', so I dropped them."""

# Drop duplicated user data
new_users_df = users_df.drop_duplicates(subset=['id', 'createdDate'], ignore_index=True)

# Check duplicated new users data
# Check missing values in each column
# Confirm id can be the primary key of users
print("Duplicated values in specific columns : \n" , new_users_df.duplicated(subset=['id']).sum())
print("Missing values in every column : \n" , new_users_df.isnull().sum())

# Select essential fields
selected_users_df = new_users_df[['id', 'createdDate']]

# Save dataframes to csv files
selected_brands_df.to_csv('selected_brands.csv', index=False)
selected_users_df.to_csv('selected_users.csv', index=False)
selected_receipts_df.to_csv('selected_receipts.csv', index=False)
group_receipt_items_df.to_csv('selected_receipt_items.csv', index=True, index_label='id')
