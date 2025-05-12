import pandas as pd
import os
import csv
from math import cos, asin, sqrt, pi
from scipy import spatial

os.getcwd()
os.chdir('map_matching')
###############################################################################
### PART 1: Retrieve stop_id according to latlong coordinates from user
### PART 2: Retrieve trip_id according to stop_id and departure_time (time is achieved one time through online code file and onetime through GPS file)
### PART 3: Retrieve route_id according to 2 trip_ids ###
### PART 4: Retrieve name and lat & lon first and last station from the stop_sequence in stop times 
### PART 5: Match the first and last lat & lon with nearest stations in GPS to get the timestamp
### PART 6: Match the timestamp of first and last station in GPS with GTFS data 
###############################################################################
### 1) stops [stop_lat & stop_lon -> stop_id] > 
### 2) stop_times [stop_id & departure_time -> trip_id] >
### 3) trips [trip_id -> route_id] > 
### 4) first and last station [stop times -> stop_sequence  -> first and last station]
##Then use these stop_id values to retrieve the stop_name values from the stops dataframe 
### 5) stop_lat & stop_lon for first & last-> timestamp
### 6) timestamp of first and last station in GPS -> timestamp of first and last station in GTFS
###############################################################################

### PART 1: Retrieve stop_id according to latlong coordinates  ###
stops = pd.read_table("stops.txt", delimiter=",")

stops['lat_lon'] = list(zip(stops.stop_lat, stops.stop_lon)) #new col (lat,long)  
stops_array = stops['lat_lon'].to_numpy() #convert series to nd array
stops_array = stops_array.tolist() #convert nd array to 2d array
tree = spatial.KDTree(stops_array) #create k-tree

    
result1 = tree.query([(43.2555761,-79.9020827)]) #query user's current lat-long
## output will represent distance between the queried point 
## and the nearest neighbour and second term is the index of the neighbour.

## result[1].item(0) would be index to search for stop_id in same df
stop_id_idx = result1[1].item(0)

stop_id2 = stops.iloc[stop_id_idx]['stop_id']

# print the result
print(result1)

## get you index
stop_id = stops["stop_id"][stop_id_idx]


### PART 2: Retrieve trip_id according to stop_id and departure_time(departure time is achieved from online data file) ###
### 2) stop_times [stop_id & departure_time -> trip_id] >

stop_times = pd.read_table("stop_times.txt", delimiter=",")

#remove colons
stop_times["departure_time"] = stop_times["departure_time"].str.replace(":","")

#create temporary df to stop all rows with stop_id value collected from Part 1
temp_df = stop_times.loc[stop_times["stop_id"] == stop_id]
temp_df.to_csv('temp_df.csv')


#get closest departure_time
#find index with minimal value by idxmin and last select by loc (time is from online code, the algorithm uses nearest time)
departure_time = (temp_df["departure_time"].astype(int)).sub(112516).abs().idxmin()
departure_time2 = (temp_df["departure_time"].astype(int)).sub(11042023104731).abs().idxmin()

#locate row based on departure_time
trip_id = stop_times.loc[departure_time]
temp_trip_id = trip_id["trip_id"]
trip_id2 = stop_times.loc[departure_time2]
temp_trip_id2 = trip_id2["trip_id"]

### PART 3: Retrieve trip_headsign according to trip_id ###
### 3) trip_id -> trip_headsigns

trips = pd.read_table("trips.txt", delimiter=",")
## find row based on trip_id
temp_trip = trips.loc[trips['trip_id'] == temp_trip_id]
trip_headsign= temp_trip["trip_headsign"].item()

temp_trip2 = trips.loc[trips['trip_id'] == temp_trip_id2]
trip_headsign2= temp_trip2["trip_headsign"].item()


### PART 4: Retrieve first and last station from the stop_sequence in stop times
##The stop with the lowest stop_sequence value for a trip is the first stop,
##The stop with the highest stop_sequence value is the last stop. 
##Then use these stop_id values to retrieve the stop_name values from the stops dataframe
### 4) first and last station [stop times -> stop_sequence  -> first and last station]
##Then use these stop_id values to retrieve the stop_name values from the stops dataframe 

# Get the stop_sequence for the selected trip_id
stop_sequence = stop_times.loc[stop_times['trip_id'] == temp_trip_id, 'stop_sequence']

# Get the stop_id of the first and last bus station
first_stop_id = stop_sequence[stop_sequence == stop_sequence.min()].index[0]
last_stop_id = stop_sequence[stop_sequence == stop_sequence.max()].index[0]

# Get the name of the first and last bus station
first_stop_name = stops.loc[stops['stop_id'] == stop_times.loc[first_stop_id, 'stop_id'], 'stop_name'].item()
last_stop_name = stops.loc[stops['stop_id'] == stop_times.loc[last_stop_id, 'stop_id'], 'stop_name'].item()

print(f"First bus station: {first_stop_name}")
print(f"Last bus station: {last_stop_name}")

# search for stop_lat by stop_name
stop_row1 = stops.loc[stops['stop_name'] == first_stop_name]
first_stop_lat = stop_row1.iloc[0]['stop_lat']
stop_row2 = stops.loc[stops['stop_name'] == first_stop_name]
first_stop_lon = stop_row2.iloc[0]['stop_lon']
stop_row3 = stops.loc[stops['stop_name'] == last_stop_name]
last_stop_lat = stop_row3.iloc[0]['stop_lat']
stop_row4 = stops.loc[stops['stop_name'] == last_stop_name]
last_stop_lon = stop_row4.iloc[0]['stop_lon']

### PART 5: Match the first and last lat & lon with nearest stations in GPS to get the timestamp
### 5) stop_lat & stop_lon for first & last-> timestamp
import json

import pandas as pd

# Open the JSON file and load into a Pandas DataFrame
with open('wayfinding-trip-locations.json', 'r') as f:
    data = pd.read_json(f)

from scipy import spatial

##Nearest Stops to the GPS Data
stops2  = list(zip(data.latitude, data.longitude)) #new col (lat,long) 
tree2 = spatial.KDTree(stops2) #create k-tree

result_first = tree2.query([(first_stop_lat,first_stop_lon)]) #query user's current lat-long
## output will represent distance between the queried point 
## and the nearest neighbour and second term is the index of the neighbour.
## result[1].item(0) would be index to search for stop_id in same df
first_stop_id_idx = result_first[1].item(0)

# Retrieve the lat_lon pair corresponding to the nearest neighbor
matched_first_lat_lon = stops2[first_stop_id_idx]

print(matched_first_lat_lon)


result_last = tree2.query([(last_stop_lat,last_stop_lon)]) #query user's current lat-long
## output will represent distance between the queried point 
## and the nearest neighbour and second term is the index of the neighbour.
## result[1].item(0) would be index to search for stop_id in same df
last_stop_id_idx = result_last[1].item(0)

# Retrieve the lat_lon pair corresponding to the nearest neighbor
matched_last_lat_lon = stops2[last_stop_id_idx]

print(matched_last_lat_lon)





### PART 6: Match the timestamp of first and last station in GPS with GTFS data
### 6) timestamp of first and last station in GPS -> timestamp of first and last station in GTFS


time1 = data.loc[data['latitude'] == matched_first_lat_lon[0], 'time'].values[0]

print(time1)


time2 = data.loc[data['latitude'] == matched_last_lat_lon[0], 'time'].values[0]

print(time2)

##2023-04-18 11:25:16 is the nearest time from GTFS and it is the same time we used for retrieving trip_ id, so the algorithm works fine.









###Extra things, not needed

# # Sort the list of dictionaries based on the 'time' key
# sorted_data = sorted(data, key=lambda x: x['time'])

# # Get the latitude and longitude of the first dictionary in the sorted list
# first_lat = sorted_data[0]['latitude']
# first_lon = sorted_data[0]['longitude']

# # Get the latitude and longitude of the last dictionary in the sorted list
# last_lat = sorted_data[-1]['latitude']
# last_lon = sorted_data[-1]['longitude']

# # Print the latitude and longitude of the first and last trips
# print(f"Latitude and Longitude of the First Trip: ({first_lat}, {first_lon})")
# print(f"Latitude and Longitude of the Last Trip: ({last_lat}, {last_lon})")

# first_trip_id = data[0]["tripId"]
# last_trip_id = data[0]["tripId"]
# first_stop_time = data[0]["time"]
# last_stop_time = data[0]["time"]




#route_id = temp_trip["route_id"].item()
### PART 5: Retrieve route_short_name according to route_id ###
# ### 5) routes [route_id -> route_short_name (& optional: route_long_name) ] 

# routes = pd.read_table("routes.txt", delimiter=",")

# ## find row based on route_id
# temp_route = routes.loc[routes["route_id"] == route_id]
# route_short_name=temp_route["route_short_name"].item()
# route_long_name = temp_route["route_long_name"].item()
# route_name = f"{route_short_name}: {route_long_name}"
# print(route_name)

# ### GRAVEYARD ###
# ## Subtract value by sub, get absolute values by abs, 
# ## find index with minimal value by idxmin and last select by loc
# #43.2603937, -79.8914996,
# #43.2603539, -79.8913434
# lat_idx = stops["stop_lat"].sub(43.2603937).abs().idxmin()
# lon_idx = stops["stop_lon"].sub(-79.8914996).abs().idxmin()

# stops_lat_idx = stops.loc[lat_idx]
# stops_lon_idx = stops.loc[lon_idx]
# print (stops_lat_idx)
# print (stops_lon_idx)


