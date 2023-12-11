#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np


# In[4]:


df1 = pd.read_csv(r"C:\Users\Sukhmani\Downloads/dataset-1.csv")


# In[5]:


df1.head()


# In[ ]:


#Q1: Under the function named generate_car_matrix 
#write a logic that takes the dataset-1.csv as a DataFrame. 
#Return a new DataFrame that follows the following rules:

#values from id_2 as columns
#values from id_1 as index
#dataframe should have values from car column
#diagonal values should be 0.


# In[12]:


def generate_car_matrix(dataset):
    pivot_df = dataset.pivot(index='id_1', columns='id_2', values='car').fillna(0)
    np.fill_diagonal(pivot_df.values, 0)  #diagonal values=0
    return pivot_df


# In[13]:


result = generate_car_matrix(df1)
print(result)


# In[14]:


#Q2: Car Type Count Calculation
#Create a Python function named get_type_count that takes the dataset-1.csv 
#as a DataFrame. Add a new categorical column car_type based on values of 
#the column car:

#low for values less than or equal to 15,
#medium for values greater than 15 and less than or equal to 25,
#high for values greater than 25.
#Calculate the count of occurrences for each car_type category and 
#return the result as a dictionary. 
#Sort the dictionary alphabetically based on keys.


# In[25]:


def get_type_count(dataset):
    df = dataset.copy()
    df['car_type'] = pd.cut(df['car'], bins=[-float('inf'), 15, 25, float('inf')], labels=['low', 'medium', 'high'], right=False)
    type_count = df['car_type'].value_counts().to_dict() #to cal count of occurences
    sort_type_count = {k:type_count[k] for k in sorted(type_count)}
    return df, sort_type_count


# In[26]:


updated_df, result2 = get_type_count(df1)
print(result2)


# In[27]:


print(updated_df)


# In[28]:


#Question 3: Bus Count Index Retrieval
#Create a Python function named get_bus_indexes that takes the dataset-1.csv 
#as a DataFrame. The function should identify and return the indices as a 
#list (sorted in ascending order) where the bus values are greater than 
#twice the mean value of the bus column in the DataFrame.


# In[32]:


def get_bus_indexes(dataset):
    bus_mean = dataset['bus'].mean()
    bus_indexes = dataset[dataset['bus']>2 * bus_mean].index.tolist()
    bus_indexes.sort()
    return dataset, bus_indexes


# In[36]:


result3 = get_bus_indexes(df1)
updated_df = result3[0]
indices = result3[1]
print(result3)


# In[37]:


#Question 4: Route Filtering
#Create a python function filter_routes that takes the dataset-1.csv as a 
#DataFrame. The function should return the sorted list of values of column 
#route for which the average of values of truck column is greater than 7.


# In[46]:


def filter_routes(dataset):
    #group by ROUTE; then cal mean of TRUCK for each route
    route_truck_means = dataset.groupby('route')['truck'].mean()
    filtered_routes = route_truck_means[route_truck_means>7].index.tolist()
    filtered_routes.sort()
    return filtered_routes


# In[47]:


result4 = filter_routes(df1)
print(result4)


# In[41]:


#Question 5: Matrix Value Modification
#Create a Python function named multiply_matrix that takes the resulting 
#DataFrame from Question 1, as input and modifies each value according to 
#the following logic:

#If a value in the DataFrame is greater than 20, multiply those values by 0.75,
#If a value is 20 or less, multiply those values by 1.25.
#The function should return the modified DataFrame which has values rounded 
#to 1 decimal place.


# In[44]:


def multiply_matrix(dataset):
    modified_df=dataset.applymap(lambda x: x*0.75 if x>20 else x*1.25)
    modified_df = modified_df.round(1)
    return modified_df


# In[45]:


result5 = multiply_matrix(result)
print(result5)


# In[48]:


#Question 6: Time Check
#You are given a dataset, dataset-2.csv, containing columns id, id_2, and 
#timestamp (startDay, startTime, endDay, endTime). The goal is to verify the 
#completeness of the time data by checking whether the timestamps for each 
#unique (id, id_2) pair cover a full 24-hour period (from 12:00:00 AM to 
#11:59:59 PM) and span all 7 days of the week (from Monday to Sunday).

#Create a function that accepts dataset-2.csv as a DataFrame and returns a 
#boolean series that indicates if each (id, id_2) pair has incorrect timestamps.
#The boolean series must have multi-index (id, id_2).


# In[49]:


df2 = pd.read_csv(r"C:\Users\Sukhmani\Downloads/dataset-2.csv")


# In[50]:


df2.head()


# In[56]:


def verify_time_completeness(dataset):
    dataset['start']=pd.to_datetime(dataset['startDay'] + '' + dataset['startTime'], errors='coerce')
    dataset['end']=pd.to_datetime(dataset['endDay'] + '' + dataset['endTime'], errors='coerce')
    
    invalid_start = dataset[dataset['start'].isnull()]
    invalid_end = dataset[dataset['end'].isnull()]
    
    if not invalid_start.empty or not invalid_end.empty:
        print("Invalid timestamps found:")
        print("Invalid start timestamps:")
        print(invalid_start)
        print("Invalid end timestamps:")
        print(invalid_end)
        return None
    
    start_valid = (dataset['start'].dt.time >= pd.Timestamp('00:00:00').time()) & (dataset['start'].dt.time <= pd.Timestamp('11:59:59').time())
    end_valid = (dataset['end'].dt.time >= pd.Timestamp('00:00:00').time()) & (dataset['end'].dt.time <= pd.Timestamp('23:59:59').time())
    
    start_end_days = dataset.groupby(['id', 'id_2'])['startDay', 'endDay'].nunique()  # Count unique start and end days
    
    valid_timestamps = start_valid & end_valid & (start_end_days['startDay'] == 7) & (start_end_days['endDay'] == 7)
    
    return valid_timestamps
    
    dataset['duration']=dataset['end']-dataset['start']
    grouped=dataset.groupby(['id', 'id_2'])
    time_check=grouped.apply(lambda x: x['duration'].sum() != pd.Timedelta(days=7))
    return time_check


# In[57]:


result6 = verify_time_completeness(df2)
print(result6)


# In[55]:


#No incorrect timestamps found, thats why 'NONE'


# In[60]:


if result6 is not None:
    invalid_start = df2[~result6 & ((df2['start'] < pd.Timestamp('00:00:00')) | (df2['start'] > pd.Timestamp('11:59:59')))]
    invalid_end = df2[~result6 & ((df2['end'] < pd.Timestamp('00:00:00')) | (df2['end'] > pd.Timestamp('23:59:59')))]

    if not invalid_start.empty or not invalid_end.empty:
        print("Invalid timestamps found:")
        print("Invalid start timestamps:")
        print(invalid_start)
        print("Invalid end timestamps:")
        print(invalid_end)
    else:
        print("All timestamps are valid.")
else:
    print("No valid timestamps found.")


# In[ ]:




