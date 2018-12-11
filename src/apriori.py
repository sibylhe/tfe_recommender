# apriori    
# - rule-based, find frequent itemsets        
# - apriori: basic, but too complex if dataset is large. Confidence and lift to be updated.    
# - toivonen: works for larger dataset. To be updated

import numpy as np
import pandas as pd

basket_gb = pd.read_csv('basket_gb.csv')

baskets = []
for row in basket_gb.itertuples():
    basket = list(row.product_id)
    baskets.append(basket)

n = len(baskets)

# find frequent 1-itemsets
def get_L1(sample, toivonen=False):
    count = {}
    for basket in sample:
        for item in basket:
            count[item] = count.get(item,0) + 1
    L1 = []
    for item, sup in count.items():
        if sup >= s:
            L1.append((item,))
    return L1


# find freq k-itemsets
def scan(dataset, Ck, minSup):
    count = {}
    for basket in dataset:
        for can in Ck:
            if set(can).issubset(basket):
                count[can] = count.get(can,0) + 1
    Lk = []
    for can, sup in count.items():
        if sup >= minSup:
            Lk.append(can)
    return Lk


# generate candidate (k+1)-itemsets from Lk
def aprioriGen(Lk, k):
    C = []
    for i in range(len(Lk)):
        for j in range(i+1, len(Lk)):
            # if L1 and L2 have (k-1) common items, combine them
            L1 = Lk[i][:k-2]
            L2 = Lk[j][:k-2]
            if L1 == L2:
                C.append(tuple(sorted(list(set(Lk[i])|set(Lk[j])))))
    return C

def apriori(sample):
    # L - all freguent itemsets
    # k=1, find frequent 1-itemsets L1
    L1 = get_L1(sample)
    L = [L1]
    L_flat = L1
    
    # k>=2, find frequent k-itemsets Lk
    k = 2
    while (len(L[k-2]) > 0):
        Ck = aprioriGen(L[k-2], k)
        if len(Ck) == 0:
            break
        Lk = scan(sample, Ck, s)
        if len(Lk) == 0:
            break
        if len(Lk) > 0:
            L.append(Lk)
            L_flat += Lk
            k += 1
    return L_flat


t = 0.05  #threshold
s = n*t
fi = apriori(baskets)

print(fi)