from pyproj import Transformer
import pandas as pd

def TransformCords(file_path):
    itm_srid = 2039
    wgs84_srid = 4326
    # ics_srid = 28193
    # utm_srid = 32636
    transformer = Transformer.from_crs(itm_srid, wgs84_srid)
    data = pd.read_csv(file_path, encoding="utf8")
    newdata = pd.DataFrame(columns=('ID','x_orig','y_orig','x_transformed','y_transformed'))

    for row in range(0,data.shape[0]): ###for n of rows
        row_id = data.loc[[row],'ID'].item()
        x = data.loc[[row],'X-orig']
        x = x.item()
        y = data.loc[[row],'Y-orig']
        y = y.item()
        points = [(x, y), ]
        for pt in transformer.itransform(points):
            '{:.3f} {:.3f}'.format(*pt)
            print(row_id, pt)
            x_transformed = pt[1]
            y_transformed = pt[0]
            list= [row_id, x,y, x_transformed, y_transformed]
            newdata.loc[row]= list


    xlwriter = pd.ExcelWriter('new_data.xlsx')
    newdata.to_excel(xlwriter, sheet_name='newdata', index=False)
    xlwriter.close()
    print('done')

file_path = r"C:\Users\michalsh\Documents\Programs and programming\for git\coords_to_transform.csv"
TransformCords(file_path)