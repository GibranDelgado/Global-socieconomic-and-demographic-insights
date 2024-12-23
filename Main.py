import sys
import os

if __name__ ==  '__main__':
    path = os.path.dirname(os.path.abspath('Main.py')) + '\\'
    sys.path.insert(0, os.path.join(os.path.dirname(sys.path[0]),f"{path}Scripts"))
    
    from Data_views import DataImport, CreateViews
    
    inputPath = f'{path}Data sources\\'
    outputPath = f'{path}Resulting files\\'
    
    if not os.path.exists(outputPath):
        os.makedirs(outputPath)
    
    DFs = DataImport(inputPath).get_dataframes()
    CreateViews(outputPath).views_creating(DFs)