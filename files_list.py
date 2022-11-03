import os
from xlrd import open_workbook
import xlwt

wb = open_workbook(file_path)
        book = xlwt.Workbook(encoding="utf-8")
        sheet1 = book.add_sheet("Sheet 1",cell_overwrite_ok=True)


        s=wb.sheets()[0]
        for col in range(s.ncols):
            print(col)
            l = []
            col_title = s.cell(0,col).value
            for row in range(s.nrows):
                if s.cell(row,col).value!="" and s.cell(row,col).value!=col_title:
                    l.append(s.cell(row,col).value)

            set_l = set(l)
            sheet1.write(0, col, col_title)
            for row_index, row_data in enumerate(set_l):
                sheet1.write(row_index+1, col, row_data)

        book.save(new_file_path)
        return "done"