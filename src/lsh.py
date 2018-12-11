# lsh  
# - user-based    
# - item-based: to be updated

import numpy as np
import pandas as pd
from datasketch import MinHashLSHForest, MinHash, MinHashLSH
import random
import time
import sys, io
from pyspark import SparkContext

product_order_gb = pd.read_csv('product_order_gb.csv')
clipboard_gb = pd.read_csv('clipboard_gb.csv')

user_dict = {}    #email -> id
up = {}    #user-product dict
data = []    #for pyspark to paralize

i = 0
for i in range(len(product_order_gb)):
    user_dict[i] = product_order_gb.iloc[i][0]
    p = sorted(list(product_order_gb.iloc[i][1])) 
    up[i] = p
    data.append((i,tuple(p)))

pu = {} #product-user dict
data2 = []

for user, products in up.items():
    for p in products:
        if p not in pu.keys():
            pu[p] = [user]
        else:
            pu[p].append(user)

row_hash = {}
rp = len(pu) #>=number of unique products
ru = len(up) #>=number of unique users

# user-based
for x in range(rp): 
    f = []
    for i in range(20):
        f.append((3*x + 13*i) % 100)    #hash function
    row_hash[x] = f

def compute_sig(products): #products: a list of products
    sig = np.ones(20, int)*int('inf')  #initial signature
    for p in products:
        h = row_hash[p]
        for i in range(20):
            if h[i] < sig[i]: 
                sig[i] = h[i]
    return tuple(list(sig))

sc = SparkContext()
UP = sc.parallelize(data, 1)
SIG = UP.mapValues(compute_sig)

r = 4 #number of rows in a band
b = 5 #number of bands

bands = SIG.map(lambda x: (x[1][0:r], x[0]))
for i in range(1,b):
    band = SIG.map(lambda x: (x[1][r*i:r*(i+1)], x[0]))
    bands = bands.union(band)
    
#candidate pairs: agree on all rows of >=1 bands
can = bands.groupByKey().map(lambda x: (x[0], sorted(list(x[1])))).collect()

# turn list of >= 3 users to pairs
canPairs = []
for c in can:
    if len(c[1]) == 2: canPairs.append(tuple(c[1]))
    if len(c[1]) > 2:
        for i in range(len(c[1])-1):
            for j in range(i+1, len(c[1])):
                canPairs.append((c[1][i],c[1][j]))
canPairs = list(set(canPairs)) #remove duplicate pairs

def jaccard_similarity(a,b):
    c = set(a).intersection(set(b))
    return float(len(c)) / (len(a) + len(b) - len(c))

#scan user-product dict to compute jaccard similarities of candidate pairs in canPairs
similarity = np.empty([len(canPairs),3]) #[user, user, sim]
for i in range(len(canPairs)):
    cp = canPairs[i]
    a = up[cp[0]]
    b = up[cp[1]]
    sim = jaccard_similarity(a,b)
    similarity[i] = np.array([cp[0],cp[1],sim])

recProducts = {} #key: cadidate user; value: similarity between the candidate user and the target user
for u in um.keys():
    canUsers = {}  #key: cadidate user; value: similarity between the candidate user and the target user
    for i in range(similarity.shape[0]):
        if u == similarity[i,0]:
            canUsers[similarity[i,1]] = sim
        if u == similarity[i,1]:
            canUsers[similarity[i,0]] = sim
    if len(canUsers) > 5: #sort cadidates by similarity, output top5: topUsers
        topUsers = [t[0] for t in sorted(canUsers.items(), key=lambda item: item[1], reverse=True)[:5]]
    else: topUsers = list(canUsers.keys())
    
    topProducts = {}  #key: product ordered by topUser; value: count
    for user in topUsers:
        for p in up[user]:
            topProducts[p] = topProducts.get(p,0)+1
        if len(topProducts) >5: #sort by count, output top5 product: recProd
            recProducts[p] = [m[0] for m in sorted(topProducts.items(), key=lambda item: item[1], reverse=True)[:5]]
        else:
            recProducts[p] = list(recProducts.keys())
recommend = [(p,recProducts[p]) for p in sorted(recProducts.keys())]
with open('user_based_results.txt','w') as out:
    out.writelines('%s,%s\n' % (user_dict[up[0]],str(up[1]).strip('[|]').replace(' ', '')) for up in recommend if len(up[1])>0)
