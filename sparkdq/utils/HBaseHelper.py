import json


def row_transform(row_cells_info, hbase_structure):
    row_cell_info_list = [json.loads(i) for i in row_cells_info]
    row_dict = {}

    hbase_index = 0
    for cell_index in range(len(row_cell_info_list)):
        column_name = row_cell_info_list[cell_index]['qualifier']
        column_value = row_cell_info_list[cell_index]['value']
        if hbase_structure[hbase_index] == column_name:
            row_dict[column_name] = column_value
            hbase_index += 1
        else:
            row_dict[hbase_structure[hbase_index]] = "Null"
            for j in range(hbase_index + 1, len(hbase_structure)):
                if hbase_structure[j] == column_name:
                    row_dict[column_name] = column_value
                    hbase_index = j + 1
                    break
                else:
                    row_dict[hbase_structure[j]] = "Null"
    for j in range(hbase_index, len(hbase_structure)):
        row_dict[hbase_structure[j]] = "Null"
    return row_dict
