# Created by Mitana Chien
# Data Analysis

import pandas as pd
from pandasql import sqldf
import sqlite3
import time

# Read data
brands_df = pd.read_csv('selected_brands.csv')
receipts_df = pd.read_csv('selected_receipts.csv')
receipt_items_df = pd.read_csv('selected_receipt_items.csv')
users_df = pd.read_csv('selected_users.csv')

brands_df.info()

brands_df = brands_df.astype(str)
brands_df.info()

receipts_df.info()

receipt_items_df.info()

users_df.info()

pysqldf = lambda q: sqldf(q, globals())

# What are the top 5 brands by receipts scanned for most recent month?
# First see the date scanned in the most recent month
q = """SELECT dateScanned, barcode, brandCode, counts FROM receipts_df rc
LEFT JOIN receipt_items_df rw
ON rc.id = rw.receiptId
WHERE datescanned BETWEEN (SELECT DATE((SELECT MAX(dateScanned) FROM receipts_df), '-1 month')) AND (SELECT MAX(dateScanned) FROM receipts_df);"""
recent_transactions = pysqldf(q)
recent_transactions

# There is only one top brand, Viva, was scanned on the receipts for most recent month.
q = """SELECT name AS recentMonthNames, CAST(SUM(counts) AS int) AS recentMonthCount FROM brands_df b
JOIN recent_transactions r
ON (b.barcode = r.barcode
OR b.brandCode = r.brandCode)
GROUP BY name
ORDER BY recentMonthCount DESC LIMIT 5;"""
top_5_brands_most_recent_month = pysqldf(q)
top_5_brands_most_recent_month

# How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
# First select top brands which were scanned on the receipts for most recent month.
q = """SELECT name AS previousMonthsNames, CAST(SUM(counts) AS int) AS previousMonthsCount FROM brands_df b
JOIN (SELECT dateScanned, barcode, brandCode, counts FROM receipts_df rc
JOIN receipt_items_df rw
ON rc.id = rw.receiptId
WHERE datescanned < (SELECT DATE((SELECT MAX(dateScanned) FROM receipts_df), '-1 month'))) AS t
ON (b.barcode = t.barcode
OR b.brandCode = t.brandCode)
GROUP BY name
ORDER BY previousMonthsCount DESC LIMIT 5;"""
top_5_brands_previous_months = pysqldf(q)
top_5_brands_previous_months

# Combine recentMonthNames and previousMonthsNames to see the results
# The ranking of the top 5 brands by receipts scanned for the recent month is different from the ranking of the top 5 brands by receipts scanned for the previous month. 
q = """SELECT recentMonthNames, previousMonthsNames FROM top_5_brands_most_recent_month
JOIN top_5_brands_previous_months;"""
top_5_brands_comparation = pysqldf(q)
top_5_brands_comparation

q = """SELECT rewardsReceiptStatus, AVG(totalSpent) AS avgSpend FROM receipts_df
GROUP BY rewardsReceiptStatus;"""
out = pysqldf(q)
out

# When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
# Since there are no 'Accepted' column in 'rewardsReceiptStatus’, I think 'Finished' is kind of 'Accepted'.
# The average spend with status of Finished(Accepted) is greater than Rejected.
q = """SELECT rewardsReceiptStatus, AVG(totalSpent) AS avgSpend FROM receipts_df
WHERE rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
GROUP BY rewardsReceiptStatus;"""
average_spend = pysqldf(q)
average_spend

# When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
# Since there are no 'Accepted' column in 'rewardsReceiptStatus’, I think 'Finished' is kind of 'Accepted'.
# The total number of items purchased with status of Finished(Accepted) is greater than Rejected
q = """SELECT rewardsReceiptStatus, CAST(SUM(purchasedItemCount) AS int) AS totalItemPurchased FROM receipts_df
WHERE rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
GROUP BY rewardsReceiptStatus;"""
total_item_purchased = pysqldf(q)
total_item_purchased

# Which brand has the most spend among users who were created within the past 6 months?
# First select receipts data of users within the past 6 months
q = """SELECT r.id, totalSpent, createdDate FROM receipts_df r
LEFT JOIN users_df u
ON r.userId = u.id
WHERE createdDate BETWEEN (SELECT DATE((SELECT MAX(createdDate) FROM users_df), '-6 month')) AND (SELECT MAX(createdDate) FROM users_df)
ORDER BY createdDate DESC;"""
users_in_6_mounths = pysqldf(q)
users_in_6_mounths

# Join repeipt, receipt_item, and brands
# Pepsi has the most spend among users who were created within the past 6 months.
q = """SELECT name AS brandName, SUM(totalSpent) AS spend FROM (SELECT barcode, brandCode, totalSpent FROM users_in_6_mounths u
LEFT JOIN receipt_items_df r
ON u.id = r.receiptId) AS t
JOIN brands_df b
ON (b.barcode = t.barcode
OR b.brandCode = t.brandCode)
GROUP BY name
ORDER BY spend DESC LIMIT 1;"""
brand_with_most_spend = pysqldf(q)
brand_with_most_spend

# Which brand has the most transactions among users who were created within the past 6 months?
# Pepsi has the most transactions among users who were created within the past 6 months.
q= """SELECT name AS brandName, COUNT(*) AS amountOfTransactions FROM (SELECT barcode, brandCode FROM users_in_6_mounths u
LEFT JOIN receipt_items_df r
ON u.id = r.receiptId) AS t
JOIN brands_df b
ON (b.barcode = t.barcode
OR b.brandCode = t.brandCode)
GROUP BY name
ORDER BY amountOfTransactions DESC LIMIT 1;"""
brand_with_most_transactions = pysqldf(q)
brand_with_most_transactions
