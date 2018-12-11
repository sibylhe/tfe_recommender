
# data preprocessing:    
# - remove test accounts    
# - aggregate data and output csv for next step

import numpy as np
import pandas as pd

test_accounts = []
with open('test_accounts.txt','r') as f:
    lines = f.readlines()
    for line in lines:
        test_accounts.append(line.strip())
print(str(len(test_accounts))+' test accounts to be removed across all relations')

def remove_accounts(df, by_col):
    df = df[~df[by_col].isin(test_accounts)]
    return df

users = pd.read_csv('tfe_user_1128.csv')
users = remove_accounts(users,'email')

product_order = pd.read_csv('product_order_1128.csv')
product_order = remove_accounts(product_order,'cr_by')

clipboard = pd.read_csv('clipboard_list_item_1128.csv')
clipboard = remove_accounts(clipboard,'cr_by')


# for lsh
product_order_notnull = product_order[product_order['source_product_id'].notnull()]
product_order_notnull.rename(columns={'id': 'product_order_id', 'source_product_id':'product_id'}, inplace=True)
product_order_notnull['product_order_id']=product_order_notnull['product_order_id'].map(lambda x: int(x))
product_order_notnull['product_id'] = product_order_notnull['product_id'].map(lambda x: int(x))
product_order_gb = product_order_notnull.groupby(['cr_by'])['product_id'].apply(set).to_frame().reset_index()

clipboard_notnull = clipboard[clipboard['product_id'].notnull()]
clipboard_gb = clipboard_notnull.groupby(['cr_by'])['product_id'].apply(set).to_frame().reset_index()

product_order_gb.to_csv('product_order_gb.csv', index=False)
clipboard_gb.to_csv('clipboard_gb.csv', index=False)


# for apriori
order1 = pd.read_csv('tfe_order_backup_1128.csv')
order2 = pd.read_csv('tfe_order_1128.csv')
order2 = order2[order2['id']>1603]
order_cols = ['id','cr_by','cr_date','buyer_email','vendor_id','old_vendor_id','order_no']
order = pd.concat([order1[order_cols],order2[order_cols]], axis=0)
order = remove_accounts(order,'cr_by')
order = remove_accounts(order,'buyer_email')

order_entry1 = pd.read_csv('order_entry_backup_1128.csv')
order_entry2 = pd.read_csv('order_entry_1128.csv')
order_entry2 = order_entry2[order_entry2['id']>525]
oe_cols = ['id','product_order_id','order_id']
order_entry = pd.concat([order_entry1[oe_cols],order_entry2[oe_cols]], axis=0)

order.rename(columns={'id': 'order_id'}, inplace=True)
basket = order[['order_id','cr_by']].merge(order_entry[['order_id','product_order_id']], on='order_id',how='left').reset_index()
basket_notnull = basket[basket['product_id'].notnull()]
basket_gb = basket_notnull.groupby('order_id')['product_id'].apply(set).to_frame().reset_index()

basket_gb.to_csv('basket_gb.csv', index=False)
