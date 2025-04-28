#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Run this cell and restart the kernel
#!pip install pandas==1.3.4


# In[ ]:


import pandas as pd
from sklearn.model_selection import train_test_split
import numpy as np


# In[ ]:


df = pd.read_csv("/home/data/new_joiner_data/train_final.csv")
df = df.drop(['id'],axis=1)
df = df.fillna(0)


# In[ ]:


df.columns


# In[ ]:


train, test = train_test_split(df,test_size=0.30)


# In[ ]:


df_out = pd.DataFrame(columns = ['variable','band','total','events','event_rate'])


# In[ ]:


def woe_cal(df_cal,var,target,decile=5):
    df_cal  = df_cal[[var,target]].reset_index()
    q   = df_cal[var].quantile(np.linspace(0,1,decile+1))
    c   = np.unique(q)
    df_cal[var+'_band'] = pd.cut(df_cal[var],c,include_lowest=True,labels=range(1,len(c)))
    df_grp=df_cal.groupby(var+'_band').agg(
    total=(target,'count'),
    events=(target,'sum'),
    b_min=(var,'min'),
    b_max=(var,'max')    
    ).reset_index()
    df_grp['band']=df_grp["b_min"].astype(str) +" - " + df_grp["b_max"].astype(str)
    df_grp['event_rate']=df_grp['events']/df_grp['total']
    df_grp['variable']=var
    return df_grp[['variable','band','total','events','event_rate']]


# In[ ]:


var_list=['x1','x2','x3','x4']
for i in var_list:
    df_out_t=woe_cal(df,i,'Target')
    df_out_t=df_out.copy()
    df_out=pd.concat([df_out,df_out_t],axis=0)
    


# In[ ]:


q   = train['x9'].quantile(np.linspace(0,1,11,0.1))
c   = np.unique(q)
train['x9_band'] = pd.cut(train['x9'],c,include_lowest=True,labels=range(1,len(c)))


# In[ ]:


df_out=train.groupby('x9_band').agg(
total=('Target','count'),
pos=('Target','sum'),
b_min=('x9','min'),
b_max=('x9','max')    
)


# In[ ]:


df_out=df_out.reset_index()


# In[ ]:


df_out['x1_woe']=df_out["b_min"].astype(str) +" - " + df_out["b_max"].astype(str)


# In[ ]:


df_out


# In[ ]:


df_out['neg']=df_out['total']-df_out['pos']
df_out['neg_per']=df_out['neg']/df_out['neg'].sum()
df_out['pos_per']=df_out['pos']/df_out['pos'].sum()


# In[ ]:


df_out['woe']=np.log(df_out['pos_per']/df_out['neg_per'])


# In[ ]:


df_out['iv']=df_out['woe']*(df_out['pos_per']-df_out['neg_per'])


# In[ ]:


df_out['iv'].sum()


# In[ ]:


##automatic way


# In[ ]:


#df = df.drop(['id'],axis=1)


# In[ ]:


from xverse.transformer import WOE
clf = WOE()


# In[ ]:


from xverse.feature_subset import SplitXY
clf1 = SplitXY(['Target']) #Split the dataset into X and y
x, y = clf1.fit_transform(train)


# In[ ]:


clf.fit(x,y)


# In[ ]:


woe_df = clf.woe_df


# In[ ]:


train[['x9','Target']].head(5)


# In[ ]:


train_x_woe = clf.transform(x)


# In[ ]:


train_x_woe[['x9']].head(5)


# In[ ]:


woe_df[woe_df['Variable_Name']=='x10'].head(5)


# In[ ]:


get_ipython().run_line_magic('matplotlib', 'inline')
from xverse.graph import BarCharts
a = BarCharts(bar_type='v')
a.plot(woe_df[woe_df['Variable_Name'] == 'x10'])


# In[ ]:


pd.options.display.float_format = '{:.4f}'.format


# In[ ]:


clf.iv_df.sort_values(by=['Information_Value'],ascending=False)


# In[ ]:





# In[ ]:





# In[ ]:




