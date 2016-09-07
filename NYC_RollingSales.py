
# coding: utf-8

# ## Using Pandas and Bokeh to Analyze and Visualze NYC Cooperative and Condominium Apartment Sales - PART 1

# My goal is to review the rolling NYC sales data to see what patterns emerge in the area of cooperative and condominium apartment sales.  My initial question is which cooperative and condominium apartment buildings have the most sales by number and percentage.  I hope that this will identify some interesting patterns that can be further explored.
# 
# The overall plan is below:
# 
#     1) Load the rolling sales data into a dataframe and clean it
#     2) Use information from the Department of Finance to convert the condominium lot number to building lot number 
#        so that sales per building can be determined       
#     3) Merge the data with information on number of residential units per building from the 
#        PLUTO database so that percentage sales per building can be calculated
#     4) Use "groupby" and "sort" to obtain the top 10 sales per building (by both number and 
#        percentage)
#     5) Create a series of bar charts using Bokeh to visualize this data
# 
# This notebook will cover steps 1 & 2 and a second notebook will cover steps 3-5.
# 
# First step is to load the required packages and to put the raw rolling sales into a dataframe.

# In[138]:

import sqlite3
import pandas as pd
import pandas.io.sql as pd_sql
import numpy as np

# I have previously downloaded rolling sales datedata in an excel file from 
# https://www1.nyc.gov/site/finance/taxes/property-rolling-sales-data.page

alldf=pd.read_excel('data/rollingsales_manhattanJuly2016.xls',skiprows=4, na_values=[' ',''])

# clean trailing sales column names
alldf.rename(columns={'EASE-MENT': 'EASEMENT', 'BUILDING CLASS AT PRESENT': 'BCLASS', 'APARTMENT NUMBER' : 'APT'}, inplace=True)

print(alldf.shape)


# Since our focus is on individual condominium and cooperative unit sales (not townhouses or office buildings) I will limit the data to the subset of cooperatives and condominiums. 

# In[139]:

bclass=['C6','C8','D0','D4','R4','R5']
df=alldf[alldf.BCLASS.isin(bclass)]
print(df.shape)
df.head(5)


# I have created functions to make the apartment numbers consistent (a review of the rolling sales data shows that condominium addresses have a seperate apartment field while cooperatives include the apartment in the ADDRESS field).

# In[140]:

pd.options.mode.chained_assignment = None  # default='warn'

# split apt from ADDRESS and update columns in df
cleanaddress=df['ADDRESS'].apply(lambda x: pd.Series(x.split(',')))

df['ADDRESS']=cleanaddress[0]
df['APT2']=cleanaddress[1]

def fixapt(ldf):  
    try:
        if len(ldf['APT'].strip())<1:
            ldf['APT'] = ldf['APT2'] 
        return(ldf['APT'])
    except:
        return(ldf['APT'])

def stripapt(ldf):
    x=ldf['APT']    
    try:
        x=x.strip(' ')
    except:
        x=x
    return(x)    

df['APT']=df.apply(fixapt, axis=1)  
df['APT']=df.apply(stripapt, axis=1)   


# Condominiums are treated differently from cooperatives in many ways, including the fact that condominium units are assigned their own unique lot number.  In order to analyze condominium sales on a building wide basis, we need to identify which building each condominium is in.  Luckily the Department of Finance provides data on the range of condominium unit lots which relate to each building lot.  
# 
# First will clean the lot information and create a seperate column 'condolot' to save the condo lot numbers. 

# In[141]:

# cleans lot
'''def fixlot(ldf):  
    try:
        x=ldf['LOT2'] 
        x=str(x)
        x=x.strip('()[]=-,')
        x=int(x)
    except:
        x=ldf['LOT']
    return(x)         
'''
def savecondolot(ldf):
    condoclass=['R4','R5']
    if ldf['BCLASS'] in condoclass:
        condolot = ldf['LOT']
    else:
        condolot = 0
    return(condolot)

#df['LOT']=df.apply(fixlot, axis=1)

# creates a new column in which the condo lot number is saved
df['CONDOLOT']=df.apply(savecondolot, axis=1)

#create df of just condos 
condoclass=['R4','R5']
df_condo=df[df.BCLASS.isin(condoclass)]
print(df_condo.shape)
 


# I have saved the condo lot dataframe as 'condo_blconverter.csv' and created a function which creates a column with the appropriate billing lot number from the dataframe.  Before I apply this to the dataframe I have created a condo only dataframe and after running the function I have added them back together.

# In[142]:

dfKey=pd.read_csv('processed/condo_blconverter.csv')

def addbuildinglot(ldf):
    condoclass=['R4','R5']
    if ldf['BCLASS'] in condoclass:
        B=ldf['BLOCK']
        L=ldf['LOT']
        resultall = dfKey.billlot[(dfKey.block == B) & (dfKey.hilot >= L) & (dfKey.lolot <= L)]  
        try:
            result=resultall.iloc[0]
        except:
            result=0              
    else:
        result = ldf['LOT']
        print ('else ',result)
    return(result)   

df_condo['LOT'] = df_condo.apply(addbuildinglot, axis=1)


# In[143]:

df_condo['LOT'].value_counts()


# In[144]:

df_else=df[~df.BCLASS.isin(condoclass)]
df = df_condo.append(df_else)
df.shape


# In[145]:

df.dtypes


# In[146]:

df = df.drop(['APT2'], axis=1)


# In[147]:

a=['BLOCK','LOT','APT','BUILDING CLASS CATEGORY','CONDOLOT']
df[a][1:100]


# In[148]:

print(df.shape)
df.dtypes


# In[149]:

df['LOT'].value_counts()


# In[150]:

df['LOT'] = pd.to_numeric(df['LOT'], errors='coerce')


# In[151]:

df.dtypes


# In[152]:

df.LOT = df.LOT.astype(int)


# In[153]:

df['LOT']


# In[154]:

df.dtypes


# In[155]:

df.to_csv('processed/RollingJulyProcessed.csv')

# read in all rolling cleaned from Part 1
df=pd.read_csv('processed/RollingJulyProcessed.csv',na_values=[' ',''])


# In[331]:

print(df.shape)
print (df.dtypes)
df.head(5)


# Now to calculate sales by building and sales by unit using groupby.  Once I create a groupby function I then execute a series of calculations on it. Since we are concerned with the number of occurences of each Block / Lot combination we could of used and column to calculate the length of.  I choose ['Sale Price'] because it could be interesting for future analysis.

# In[332]:

# calculate data on sales by building
bybuilding = df.groupby(['BLOCK','LOT'])
salesbybuilding = bybuilding['SALE PRICE'].agg([np.sum,np.mean,np.std,len])
salesbybuilding=salesbybuilding.rename(columns={'len':'NUMSALES'})
salesbybuilding=salesbybuilding.reset_index()

# calculate data on sales by unit
byapt = df.groupby(['BLOCK','LOT','APT'])
salesbyapt = byapt['SALE PRICE'].agg([np.sum,np.mean,np.std,len])
salesbyapt=salesbyapt.rename(columns={'len':'NUMSALES'})
salesbyapt=salesbyapt.reset_index()


# PLUTO database has nearly 50 columns of information on every property in New York City.  I have previously downloaded the data to a database and will use this to create a dataframe to add owner, # of residential units and class of property to the processed dataframe of rolling data.

# In[333]:

def getpluto():
    db = sqlite3.connect('processed/NYCPROPERTY.db')
    cursor = db.cursor()
    cursor.execute('SELECT BLOCK, LOT, OwnerName, Unitsres, BldgClass FROM condoandcoop')
    result = cursor.fetchall()
    df_coop = pd.DataFrame(result, columns=['BLOCK','LOT', 'OWNER', 'UNITSRES','CLASS'])
    cursor.close()
    df_coop =df_coop.sort_values('BLOCK').drop_duplicates(subset=['BLOCK', 'LOT'], keep='last')
    return df_coop

plutodata=getpluto()


# As shown below the plutodata uses block and lot to idnetify the buildings (similar to the df that we have been working on).  So I will use this to merge the data.  

# In[334]:

print(plutodata.shape)
print (plutodata.dtypes)
plutodata.head(5)


# Now that we have created the groupby we can add the information from the Pluto database.

# In[335]:

def fixapt(ldf):  
    if len(ldf['APT'].strip())<1:
        ldf['APT'] = ldf['APT2'] 
    return(ldf['APT'])   
        
def stripname(ldf):
    x=ldf['ADDRESS']    
    try:
        x=x.strip(' ')
        x=x.replace("STREET","ST")  
        x=x.replace("EAST","E")
        x=x.replace("WEST","W")
        x=x.replace("AVENUE","AVE")
        x=x.replace("BOULEVARD","BLVD")
        x=x.replace("SOUTH","S")             
    except:
        x=x
    return(x)
    
# Add unitsres and owner
salesbybuilding = pd.merge(salesbybuilding, plutodata, on=['BLOCK','LOT'], how='left')
salesbyapt = pd.merge(salesbyapt, plutodata, on=['BLOCK','LOT'], how='left')

# Add address and apartment information
address_df = df[['BLOCK', 'LOT','CONDOLOT','ADDRESS','BCLASS']]
address_df = address_df.sort_values('BLOCK').drop_duplicates(subset=['BLOCK', 'LOT'], keep='last')
salesbybuilding = pd.merge(salesbybuilding, address_df, on=['BLOCK','LOT'], how='left')
salesbybuilding['ADDRESS']=salesbybuilding.apply(stripname, axis=1)

# top 20 sales by number
mostsales =salesbybuilding.sort_values('NUMSALES',ascending=False).head(20)
salescoop = salesbybuilding[salesbybuilding['CONDOLOT'] <1]
mostsalescoop = salescoop.sort_values('NUMSALES',ascending=False).head(20)


# In[336]:

salesbybuilding['UNITSRES'].isnull().value_counts()


# In[337]:

salescoop.dtypes


# In[338]:

totalsales = salesbybuilding['NUMSALES'].sum()
print(totalsales)


# Now to calculate as a percentage.

# In[339]:

def getpercent(ldf):
    try:
        ldf['PERSALES'] = ldf['NUMSALES'] / ldf['UNITSRES']
    except:     
        ldf['PERSALES'] = float('nan')
    return ldf['PERSALES']

# Calculate the percentage sold and add column with info
salesbybuilding['PERSALES']=salesbybuilding.apply(getpercent, axis=1)

salesbybuilding[200:210]


# In[340]:

salesbybuilding.shape


# Going to use Bokeh library to visualize the data.

# In[341]:

from bokeh.plotting import figure, output_file, show, output_notebook
from bokeh.charts import Bar, hplot, Scatter
from bokeh.models import HoverTool
from bokeh.layouts import row, column, gridplot
import pandas.io.sql as pd_sql
from bokeh.charts.attributes import CatAttr


# In[342]:

output_notebook()

hover = HoverTool(
        tooltips=[("Building","@numsales")])
      
hover.tooltips = [('Value of ID',' $x'),('Value of Total',' @y')]
        
bar_numsales = Bar(mostsales, values='NUMSALES', label=CatAttr(columns=['ADDRESS'], sort=False), color='green', ylabel='Sales in the last 12 Months', title='Most Unit Sales Coops & Condos',legend=None,tools='hover')
#bar_numcoops = Bar(mostsalescoop, 'ADDRESS', values='numsales',color='green', ylabel='Sales in the last 12 Months', title='Most Sales by Coop Building',legend=None)

bar_numcoops = Bar(mostsalescoop, values='NUMSALES', color='green', ylabel='Sales in the last 12 Months', label=CatAttr(columns=['ADDRESS'], sort=False),title='Most Unit Sales Coop Buildings',legend=None,tools='hover')

# top 20 sales by percentage
largestper =salesbybuilding.sort_values('PERSALES',ascending=False).head(20)
largestper['ADDRESS']=largestper.apply(stripname, axis=1)
bar_persales = Bar(largestper, values='PERSALES', label=CatAttr(columns=['ADDRESS'], sort=False), color='green', ylabel='Sales in the last 12 Months', title='Greatest % Unit Sales in Coop & Condo Buildings',legend=None, tools='hover')

# top 20 sales coops by percentage
salescoop = salesbybuilding[salesbybuilding['CONDOLOT'] <1]
largest_coop_per = salescoop.sort_values('PERSALES',ascending=False).head(20)
bar_coop_persales = Bar(largest_coop_per, values='PERSALES', label=CatAttr(columns=['ADDRESS'], sort=False), color='green', ylabel='Sales in the last 12 Months', title='Greatest % Unit Sales in Coop Buildings',legend=None)

# make a grid
grid = gridplot([bar_numsales, bar_numcoops, bar_persales, bar_coop_persales], ncols=2, plot_width=400, plot_height=400)
show(grid)