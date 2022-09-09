import pandas as pd

from queue import Queue
from threading import Thread
import cdsapi
from time import time
import datetime
import os
c = cdsapi.Client()

data=pd.read_csv('../pre_processing.csv')

s_data= data.loc[data['YEAR'] >1999]
print(s_data.index.to_numpy())
# for value in s_data:
#     print(value)
index=0
for ind in s_data.index:
    # if index == 5:
    #     exit()
    row_number=ind+1
    tid=s_data['TID'][ind]
    year= s_data['YEAR'][ind]
    month=s_data['MONTH'][ind]
    day=s_data['DAY'][ind]
    h=s_data['HOUR'][ind]

    lat=s_data['LAT'][ind]
    long=s_data['LONG'][ind]
    if h<10:
        hours='0'+str(h)+":00"
    else:
        hours = str(h) + ":00"
    # calculating the co-ordinates of south , north, East and west
    north = int(lat * 0.1) + 1

    south = int((lat * 0.1) - 1)
    # if south <0:
    #     south==0
    east = int((long * 0.1) + 1)
    west = int((long * 0.1) - 1)
    name=str(year)+'_'+str(month)+'_'+str(day)+'_'+str(h)+'_'+str(row_number)
    print(name)
    print("here is all information")

    GRID = [1, 1]
    filename = "data/" +str(name) + ".nc"
    if (os.path.isfile(filename)):
        print("ok", filename)
    else:
        c.retrieve(
            'reanalysis-era5-pressure-levels',
                {
                'product_type': 'reanalysis',
                'variable': [
                'relative_humidity', 'temperature', 'u_component_of_wind',
                'v_component_of_wind',
                ],
                'pressure_level': [
                    '1', '2', '3',
                    '5', '7', '10',
                    '20', '30', '50',
                    '70', '100', '125',
                    '150', '175', '200',
                    '225', '250', '300',
                    '350', '400', '450',
                    '500', '550', '600',
                    '650', '700', '750',
                    '775', '800', '825',
                    '850', '875', '900',
                    '925', '950', '975',
                    '1000',
                ],
                'year': str(year),
                'month': str(month),
                'day': str(day),
                'time': str(hours),
                'format': 'netcdf',  # Supported format: grib and netcdf. Default: grib
                'area': [
                    #             # n,w,s,e
                     north, west, south, east,
                ],
                'grid': GRID,
            },
            "data/" + str(name)   + ".nc")  # Output file. Adapt as you wish.
    index=index+1
