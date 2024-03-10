import pandas as pd

df = pd.read_csv(r"F:\Powerbi\Data Engineer\UBer\Uber.csv")
# 'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
#        'passenger_count', 'trip_distance', 'pickup_longitude',
#        'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag',
#        'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount',
#        'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
#        'improvement_surcharge', 'total_amount'
#######################################################################

df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format= "%d-%m-%Y %H:%M")
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], format= "%d-%m-%Y %H:%M")
dtf = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
dtf["Date_ID"] = dtf.index + 1
dtf = dtf[["Date_ID",'tpep_pickup_datetime','tpep_dropoff_datetime']]

dtf["Pickup_Hour"] = dtf['tpep_pickup_datetime'].dt.hour

dtf["Pickup_Day"] = dtf['tpep_pickup_datetime'].dt.day

dtf["Pickup_Month"] = dtf['tpep_pickup_datetime'].dt.month

dtf["Pickup_Year"] = dtf['tpep_pickup_datetime'].dt.year

dtf["Pickup_WeekDay"] = dtf['tpep_pickup_datetime'].dt.weekday

# Dropoff

dtf["Dropoff_Hour"] = dtf['tpep_dropoff_datetime'].dt.hour

dtf["Dropoff_Day"] = dtf['tpep_dropoff_datetime'].dt.day

dtf["Dropoff_Month"] = dtf['tpep_dropoff_datetime'].dt.month

dtf["Dropoff_Year"] = dtf['tpep_dropoff_datetime'].dt.year

dtf["Dropoff_WeekDay"] = dtf['tpep_dropoff_datetime'].dt.weekday
# print(dtf)
dtf.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Date_Table.csv')


###################################################################################
                        #Pick
dpf = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop =True)
dpf['Pickup_ID'] = dpf.index + 1
dpf = dpf[['Pickup_ID', 'pickup_longitude', 'pickup_latitude']]
# print(dpf)
dpf.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Pickup_Table.csv')

                        #Drop
ddf = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop = True)
ddf["Dropoff_ID"] = ddf.index + 1
ddf= ddf[["Dropoff_ID", 'dropoff_longitude', 'dropoff_latitude']]
# print(ddf)
ddf.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Dropoff_Table.csv')


###############################################################################
                        #Passenger
dpaf = df[['passenger_count']].drop_duplicates().reset_index(drop = True)
dpaf['Passenger_ID'] = dpaf.index + 1
dpaf = dpaf[['Passenger_ID', 'passenger_count']]
# print(dpaf)
dpaf.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Passenger_Table.csv')

###############################################################################
                        #Trip Distance
dtd = df[['trip_distance']].drop_duplicates().reset_index(drop = True)
dtd['Trip_Distance_ID'] = dtd.index + 1
dtd = dtd[[ 'Trip_Distance_ID' ,'trip_distance']]
# print(dtd)
dtd.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Trip_Distance.csv')

################################################################################
                         #Rate Code
rate_code_type = {1 : 'Standard rate',
                  2 : 'JFK',
                  3 : 'Newark',
                  4 : 'Nassau or Westchester',
                  5 : 'Negotiated fare',
                  6 : 'Group ride'}


drc = df[['RatecodeID']].drop_duplicates().reset_index(drop = True)
drc['RateCode_Name'] = drc['RatecodeID'].map(rate_code_type)
# print(drc)
drc.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Rate_Code_Table.csv')

###############################################################################
                        #Payment Table
payment_type = {1 : 'Credit card',
                2 : 'Cash',
                3 : 'No charge',
                4 : 'Dispute',
                5 : 'Unknown',
                6 : 'Voided trip'}
dpy = df[['payment_type']].drop_duplicates().reset_index(drop = True)
dpy['payment_type_name'] = dpy['payment_type'].map(payment_type)
# print(dpy)
dpy.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Payment_Table.csv')

################################################################################
                        #Vendor Table
vendorf = df.merge(dtf, on =['tpep_pickup_datetime', 'tpep_dropoff_datetime'], how = 'inner')\
            .merge(dpf, on = ['pickup_longitude', 'pickup_latitude'], how = 'inner')\
            .merge(ddf, on = ['dropoff_longitude', 'dropoff_latitude'], how = 'inner')\
            .merge(dpaf, on = ['passenger_count'], how = 'inner')\
            .merge(dtd ,on = ['trip_distance'], how = 'inner')\
            .merge(drc, on = ['RatecodeID'], how = 'inner')\
            .merge(dpy, on = ['payment_type'], how = 'inner')

vendorf = vendorf[['VendorID', 'Date_ID', 'Passenger_ID', 'Trip_Distance_ID', 'Pickup_ID', 'Dropoff_ID', 'RatecodeID',
                   'payment_type', 'fare_amount',
                   'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                   'improvement_surcharge', 'total_amount']]
print(vendorf, "\n", vendorf.columns)
vendorf.to_csv(r'F:\Powerbi\Data Engineer\UBer\Fact_Dimensional\Vendor_Table.csv')
