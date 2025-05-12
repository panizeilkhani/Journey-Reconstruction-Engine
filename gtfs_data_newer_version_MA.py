from pymongo import MongoClient

import os
import datetime
# import the libraries 
import folium
import pandas as pd
import numpy as np
import sys
from scipy import spatial
import math
import matplotlib.pyplot as plt
print(sys.version)
print(pd.__version__)

os.getcwd()
os.chdir('map_matching')

###################################################################################################

MONGO_URI = "mongodb://mac-reader:p01VXL67XJyI@mongo.menlolab-mcmaster-cubic.com:27017/MacCubic?authSource=admin&readPreference=primary&appname=PythonGTFS&ssl=true"
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')

client = MongoClient(MONGO_URI, directConnection=True)

if MONGO_DB_NAME is not None:
    print("using db: " + MONGO_DB_NAME)
    mydb = client.get_database(MONGO_DB_NAME)
else:
    print("using db: " + "MacCubic")
    mydb = client.get_default_database()

gtfsData = mydb["hamilton-gtfs"]

# Define start and end dates
start_date = datetime.datetime(2023, 4, 25)  # it starts from 2023-04-25 00:00:00
end_date = datetime.datetime(2023, 4, 26)     # it ends at 2023-04-26 00:00:00

# Load the vehicle position data into a pandas dataframe between the two dates
df = pd.DataFrame(pd.json_normalize(list(gtfsData.find({"header.timestamp": {"$gte": start_date, "$lt": end_date}}))))

# expand the nested "entity" field into a additional rows
df = df.explode('entity')


###################################################################
# expand the json fields in the "entity" column
df = pd.concat([df.drop(['entity'], axis=1), df['entity'].apply(pd.Series)], axis=1)

# list the columns
print(df.columns)


###################################################################
# remove one of the extra id column it will be re-added later
df = df.drop(['id'], axis=1)

# expand the json fields in the "vehicle" column
df = pd.concat([df.drop(['vehicle'], axis=1), df['vehicle'].apply(pd.Series)], axis=1)

# expand the json fields in the trip vehicle and position columns
df = pd.concat([df.drop(['trip', 'vehicle', 'position'], axis=1), df['trip'].apply(pd.Series), df['vehicle'].apply(pd.Series), df['position'].apply(pd.Series)], axis=1)

print(df.columns)

# save the dataframe to a file
df.to_pickle('df_GPS_12april.pkl')

# # load the dataframe from a file
df = pd.read_pickle('df_GPS_25april.pkl')
df.head(2)

############# Retreive the GPS data ############################
# read csv file 
df_user_trips = pd.read_csv('ITEC_GoodTrips.csv')
# select the trip ID from the ITEC_GoodTrips.csv file which is GPS data collected form the user
trip_id_selected= '644819a3d544611880a103e4'
selected_user_trip = df_user_trips[df_user_trips['TripID'] == trip_id_selected]
# CI information
bearing_CI = selected_user_trip['CI bearing'].iloc[0]
lat_CI= selected_user_trip['CI_lat'].iloc[0]
lon_CI= selected_user_trip['CI_lon'].iloc[0]
CI_time= selected_user_trip['CI_time'].iloc[0]
# CO information
lat_CO= selected_user_trip['CO_lat'].iloc[0]
lon_CO= selected_user_trip['CO_lon'].iloc[0]
CO_time= selected_user_trip['CO_time'].iloc[0]


### Finding stop_id of the latitude and longtitude 
target_lat = lat_CI
target_lon = lon_CI
target_bearing= bearing_CI


#########  Can IGNORE --- This is just for sanity check-  Can ignore-   Nearest Stops to the GPS Data (only one closest point to the user GPS data)  ###############
stops_only1  = list(zip(df.latitude, df.longitude)) #new col (lat,long) 

tree = spatial.KDTree(stops_only1) #create k-tree
# result = tree.query([(43.2555761,-79.9020827)]) #query user's current lat-long
result = tree.query([(target_lat,target_lon)], k=1) #query user's current location

## result[1].item(0) would be index to search for stop_id in same df
stop_id_idx = result[1].item(0)

stop_id = df.iloc[stop_id_idx]['stopId']
target_stop_id = stop_id
print(target_stop_id)


#######################   Filter Block box of the algorithm  #########################################
###################### Finding the closest stop ids based on bearing and latitute and longtitude ############################
# Function to calculate the distance between two points
def distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the earth in km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c  # Distance in km
    return d * 1000  # Distance in meters

# Nearest Stops to the GTFS Data
stops = list(zip(df.latitude, df.longitude, df.bearing))  # (lat, long, bearing) tuples

# Calculate the distance between the target location and each stop
distances = []
for stop in stops:
    distances.append(distance(target_lat, target_lon, stop[0], stop[1]))

# Create a new column for distances in the df
df['distance'] = distances

# Sort the df by distance in ascending order
df_sorted = df.sort_values(by='distance')

# Filter the df to only include stops within a certain radius (e.g. 60 meters)
radius = 100  # in meters
df_filtered = df_sorted[df_sorted['distance'] <= radius]

unique_stopIds=set(df_filtered.stopId)
print(unique_stopIds)

# Filter the df to only include stops with the same or similar bearing
bearing_tolerance = 50  # in degrees
df_filtered = df_filtered[(df_filtered['bearing'] >= target_bearing - bearing_tolerance) &
                          (df_filtered['bearing'] <= target_bearing + bearing_tolerance)]

# Get the stop_id of the nearest stop
if not df_filtered.empty:
    stop_id = df_filtered.iloc[0]['stopId']
else:
    stop_id = None

df_filtered[['vehiclePositionObject.header.timestamp', 'stopId', 'tripId', 'routeId', 'bearing', 'latitude', 'longitude']]
print(df_filtered[['vehiclePositionObject.header.timestamp', 'stopId', 'tripId', 'routeId', 'bearing', 'latitude', 'longitude', 'distance']])


################################### This is the KD_tree block in the algorithm development  ######################################
# specify the value to check against
# time CO 15042023081140   time CO 	15042023080351
time_str = "25042023140826"
dt = datetime.datetime.strptime(time_str, "%d%m%Y%H%M%S")
formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")  #'2023-04-25 14:08:26'
value_to_check = formatted_time 
value_to_check_num = pd.to_datetime(value_to_check).timestamp()

# convert datetime to timestamp (in seconds) and then to integer
df['timestamp'] = pd.to_datetime(df['vehiclePositionObject.header.timestamp']).apply(lambda x: int(x.timestamp()))

#create temporary df to stop all rows with target stopId 
temp_df = df[df['stopId'].isin(unique_stopIds)]
# temp_df['timestamp']

# save the first 10 rows to an Excel file
# temp_df_head.to_excel('first_100_rows.xlsx', index=False)

#get closest departure_time
#find index with minimal value by idxmin and last select by loc
# departure_time_index = (temp_df['timestamp'].astype(int)).sub(value_to_check_num).abs().idxmin()
departure_time_index = (temp_df['timestamp'].astype(int)).sub(value_to_check_num).abs().nsmallest(10).index
departure_time = temp_df['timestamp'][departure_time_index]
#convert timestamp to datetime
Date_Time = pd.to_datetime(departure_time, unit='s')

#locate row based on departure_time
trip_id = temp_df['tripId'][departure_time_index]
bearing_values = temp_df['bearing'][departure_time_index]
stop_id_tosave = temp_df['stopId'][departure_time_index]
lat_tosave = temp_df['latitude'][departure_time_index]
lon_tosave = temp_df['longitude'][departure_time_index]
StopSequence_number = temp_df['currentStopSequence'][departure_time_index]
# next_StopSequence_number = temp_df['currentStopSequence'== ][departure_time_index]


# get route_id based on trip_id and departure_time
route_ids = temp_df['routeId'][departure_time_index]
route_ids_list = route_ids.tolist()
route_ids_list = [int(x) for x in route_ids_list]


#save the route_ids with their corresponding departure_time and trip_id in a dataframe
route_ids_df = pd.DataFrame({'Bus_departure_time': departure_time, 'Date': Date_Time, 'StopSequence': StopSequence_number, 'StopId': stop_id_tosave, 'latitude': lat_tosave, 'longitude': lon_tosave, 'Bearing': bearing_values, 'tripId': trip_id, 'routeId': route_ids})

# get route name based on route_id from the routes.txt file
routes_datafile = pd.read_table("routes.txt", delimiter=",")


route_names2 = []
# Loop through the list of 5 trip_ids for CO scenario
for route_id1 in route_ids_list:
    # Find the row in the routes DataFrame that matches the current route_id
    temp_route = routes_datafile.loc[routes_datafile["route_id"] == route_id1]
    # Extract the route_short_name andlong from the temp_route DataFrame
    route_short_name2 = temp_route["route_short_name"].item()
    route_long_name2 = temp_route["route_long_name"].item()
    route_name2 = f"{route_short_name2}: {route_long_name2}"
    # Append the route_name to the list of route_names
    route_names2.append(route_name2)

route_ids_df['route_name'] = route_names2
route_ids_df = route_ids_df.reset_index(drop=True)
# delete the duplicate rows based on trip_id and departure_time
route_ids_df = route_ids_df.drop_duplicates(subset=['tripId', 'Bus_departure_time'], keep='first').reset_index(drop=True)
print(route_ids_df)

##########################  Merge Block Algorithm  #########################
# find intersect of two dataframes of route_ids_df and df_filtered in column of tripId
route_ids_df = route_ids_df.reset_index(drop=True)
df_filtered = df_filtered.reset_index(drop=True)
df_intersect = pd.merge(route_ids_df, df_filtered, on=['tripId'], how='inner')  # since route_ids_df is the left dataframe, it sorts the ...
# ... merged dataframe based on the left dataframe which is based on closest timestamp to the target timestamp

# if there is mutual tripId between the two dataframes, then the merged dataframe will not be empty and we assign it to route_ids_df (which is sorted based on the time stamp)
if df_intersect.empty:
    df_intersect = route_ids_df
else:
    df_intersect = df_intersect.reset_index(drop=True)



tripId_merged = df_intersect['tripId'][0]  #closest timestamp to the target timestamp

route_name_merged = route_ids_df.loc[route_ids_df['tripId'] == tripId_merged, 'route_name'].tolist()[0]
Date_merged = route_ids_df.loc[route_ids_df['tripId'] == tripId_merged, 'Date'].tolist()[0]
Date_merged= Date_merged.strftime("%Y-%m-%d %H:%M:%S")



##################### Last station comparision with the user GPS data ############################
# Calculate the distance between the target location and each stop
distances_CO = []
for stop in stops:
    distances_CO.append(distance(lat_CO, lon_CO, stop[0], stop[1]))

# Create a new column for distances in the df
df['distance_CO'] = distances_CO

# Sort the df by distance in ascending order
df_sorted_CO = df.sort_values(by='distance_CO')

# Filter the df to only include stops within a certain radius (e.g. 60 meters)
radius = 40  # in meters
df_filtered_CO = df_sorted_CO[df_sorted_CO['distance_CO'] <= radius]

unique_stopIds_CO=list(set(df_filtered_CO.stopId))
print(unique_stopIds_CO)


################## check if the uniques trips that were found in the merged step has similar stops to the user GPS check out data ##################
unique_tripIds = df_intersect.tripId.unique()
df4=df
df4=df4.reset_index(drop=True)
mutual_buses = {}
i=0
for trip_intersected in unique_tripIds:
    temp_df4 = df4.loc[df4['tripId'] == trip_intersected]
    temp_df4 = temp_df4.reset_index(drop=True)
    stops =temp_df4["stopId"].tolist()
    mutual_values = set(unique_stopIds_CO).intersection(set(stops))
    if len(mutual_values) > 0:
        # save the tripId that has similar stops to the user GPS check out data in a new dataframe and save the mutual stops in a list
        mutual_buses [i] = pd.DataFrame({'tripId': trip_intersected, 'mutual_stops': list(mutual_values)})
        i=i+1

# convert the dictionary of mutual_buses to a dataframe
if mutual_buses == {}:  # if there is no mutual stops between the user GPS check out data and the trips that were found in the merged step
    mutual_buses_df = df_intersect # then the mutual_buses_df will be the df_intersect
    tripId_merged_CO = df_intersect['tripId'][0]  # closest timestamp to the target timestamp
else:
    mutual_buses_df = pd.concat(mutual_buses)
    mutual_buses_df = mutual_buses_df.reset_index(drop=True)
    # closest timestamp to the target timestamp
    tripId_merged_CO = mutual_buses_df['tripId'][0] 
    # find the trip that has the most mutual stops with the user GPS check out data
    # max_mutual_stops = mutual_buses_df['mutual_stops'].str.len().max() 
    # tripId_merged_CO = mutual_buses_df.loc[mutual_buses_df['mutual_stops'].str.len() == max_mutual_stops, 'tripId'].tolist()[0]


print(mutual_buses_df)

# find the trip in df_intersect that has the most mutual stops with the user GPS check out data
print(tripId_merged_CO)

# find the route name and date of the tripId_merged2
route_name_merged2 = route_ids_df.loc[route_ids_df['tripId'] == tripId_merged_CO, 'route_name'].tolist()[0]
Date_merged2 = route_ids_df.loc[route_ids_df['tripId'] == tripId_merged_CO, 'Date'].tolist()[0]
Date_merged2= Date_merged2.strftime("%Y-%m-%d %H:%M:%S")
Final_result = pd.DataFrame({'Result without CO': [route_name_merged, Date_merged], 'Result with CO': [route_name_merged2, Date_merged2]})
Final_result_CO = Final_result['Result with CO'].tolist()


###################################   Visualization ########################################
# Import the estimated bus route from the GTFS data
df3=df
df3=df3.reset_index(drop=True)
# temp_df3 = df3.loc[df3['tripId'] == tripId_merged]  # without CO
temp_df3 = df3.loc[df3['tripId'] == tripId_merged_CO]  # without CO
temp_df3 = temp_df3.reset_index(drop=True)
latitude =temp_df3["latitude"]
longitude =temp_df3["longitude"]

### import user GPS data from excel file
df_user = pd.read_csv('USER_gps.csv')
df_user.columns

# ############  Plotting on the figure  ############
fig, ax = plt.subplots(figsize=(10, 6))

ax.scatter(df_user['longitude'] , df_user['latitude'], alpha=0.15, color='blue' , label='User Route')
ax.plot(longitude, latitude, 'o', color='black', label='Bus Stops Location')
# plot red circle marker at the first point
ax.plot(df_user['longitude'][0], df_user['latitude'][0], marker='*', markersize=20, color='red', label='User Departure Point')
ax.plot(longitude[0], latitude[0], marker='*', markersize=20, markerfacecolor='green', markeredgecolor='black', label='Bus Origin Point')
ax.tick_params(axis='both', which='major', labelsize=14)

plt.xlabel('Longitude (\u00b0)', fontsize=16)
plt.ylabel('Latitude (\u00b0)', fontsize=16)
# format tick labels
plt.title(f'Departure Time: {Date_merged2}, TripID: {tripId_merged_CO}, \n Bus {route_name_merged2}' ,  fontsize=16)
plt.ticklabel_format(axis='both', style='plain', useOffset=False,  fontsize=12)
plt.gca().xaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
plt.gca().yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
plt.legend( fontsize=12)
plt.grid()
plt.show()


