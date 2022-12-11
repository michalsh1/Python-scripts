import pyodbc
import pandas as pd
import numpy as np
import geopandas

path = "C:/Users/michalsh/Downloads/res_poly.zip"
shp = geopandas.read_file(path)


for i in range(0,shp.shape[0]):
    print(shp['id'][i])
    print(shp['name'][i])
    print(shp['nice_displ'][i])
    if shp['geometry'][i] != None:
        print(shp['geometry'][i])
    else:
        print('no geometry!')

    print(shp['point'][i])
    print('-------------------')


for i in range(0,shp.shape[0]):
    if shp['geometry'][i]!= None:
        if shp['geometry'][i].type == 'MultiPolygon':
            print(shp['id'][i])
            print('-------------------')





# for i in shp['geometry']:
#     if i != None:
# for i in shp['geometry']:
#     if i != None:
        # if i.type == 'MultiPolygon'
        # if i.type == 'Polygon'
        # print(i)