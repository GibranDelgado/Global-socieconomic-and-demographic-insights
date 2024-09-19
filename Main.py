import sys
import os

if __name__ ==  '__main__':
    path = os.path.dirname(os.path.abspath('main.py'))+'\\'
    sys.path.insert(0, os.path.join(os.path.dirname(sys.path[0]),f"{path}Scripts"))
    from Data_views import Creating_data
    
    input_path_files = f'{path}Data sources\\'
    output_path_files = f'{path}\\Query_results\\'
    if not os.path.exists(output_path_files):
        os.makedirs(output_path_files)
    
    Creating_data(input_path_files, output_path_files)