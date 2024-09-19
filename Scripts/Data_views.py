import pandas as pd
import numpy as np
from Development_and_sociodemographic_index import Cleaning_demo_index, Cleaning_population_by_ages, Cleaning_multiple_demo_index, Cleaning_hours_worked

def Reading_data(path_file):
    Demo_index_src = pd.read_csv(f'{path_file}WPP2022_Demographic_Indicators_Medium.csv', 
                                 usecols=['ISO3_code','Location','LocTypeName','Time','CBR','LEx'])
    
    Population_by_ages_src = pd.read_csv(f'{path_file}WPP2022_PopulationByAge5GroupSex_Medium.csv',
                                         usecols=['ISO3_code','Location','LocTypeName','Time','AgeGrp','PopMale','PopFemale', 'PopTotal'])
    
    Multiple_demo_index_src = pd.read_csv(f'{path_file}HDR23-24_Composite_indices_complete_time_series.csv', encoding='latin-1')
    
    Hours_worked_src = pd.read_excel(f'{path_file}pwt1001.xlsx', sheet_name='Data', usecols=['countrycode','country','year','avh'])
    Regions = pd.read_csv(f'{path_file}ISO3_codes_world_regions.csv', usecols=['alpha-3','name','region','sub-region']).dropna()
    
    Demo_index_stg = Cleaning_demo_index(Demo_index_src, Regions)
    Population_by_ages_stg = Cleaning_population_by_ages(Population_by_ages_src)
    Multiple_demo_index_stg = Cleaning_multiple_demo_index(Multiple_demo_index_src, Regions, Population_by_ages_stg)
    Hours_worked_stg = Cleaning_hours_worked(Hours_worked_src)
    
    return Demo_index_stg, Population_by_ages_stg, Multiple_demo_index_stg, Hours_worked_stg

def Categorizing_index(df, indicator):
    Class_indic = pd.Series(np.zeros(len(df)))

    if indicator=='hdi':
        intervals = [0.4, 0.6, 0.75, 0.9]
    elif indicator=='gii':
        intervals = [0.15, 0.3, 0.5, 0.65]
    elif indicator=='mys':
        intervals = [2, 5, 8, 11]
    else:
        raise ValueError('Opcion no valida')
    
    very_low = df[df[indicator]<=intervals[0]].index.values
    low = df[(df[indicator]>intervals[0]) & (df[indicator]<=intervals[1])].index.values
    medium = df[(df[indicator]>intervals[1]) & (df[indicator]<=intervals[2])].index.values
    high = df[(df[indicator]>intervals[2]) & (df[indicator]<=intervals[3])].index.values
    very_high = df[df[indicator]>intervals[3]].index.values
    
    Class_indic[very_low] = 'Very low'
    Class_indic[low] = 'Low'
    Class_indic[medium] = 'Medium'
    Class_indic[high] = 'High'
    Class_indic[very_high] = 'Very high'
    Class_indic[Class_indic==0] = 'No data'
    
    return Class_indic

def Filtering_OECD_countries(Hours_worked):
    OECD = pd.DataFrame({'ISO3_code':['AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE',
                                      'DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL',
                                      'IRL','ISR','ITA','JPN','KOR','LVA','LTU','LUX',
                                      'MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN',
                                      'ESP','SWE','CHE','TUR','GBR','USA']})
    
    return Hours_worked.merge(OECD, how='inner', on='ISO3_code')

def Creating_data(input_path_files, output_path_files):
    #path_file = 'C:\\Users\\Gibran\\Documents\\Demographic info datasets\\Data Sources\\'
    Demo_index, Population_by_ages, Multiple_demo_index, Hours_worked = Reading_data(input_path_files)

    #Query 1 (Demographic index world map)
    Demo_index.iloc[:,2:].to_excel(f'{output_path_files}Q1. World map demographic indicators.xlsx', index=False)

    #Query 2 (Country population throughout the years)
    Total_population_by_country_age = Population_by_ages.groupby(['ISO3_code','Location','Time']).agg({'PopTotal':'sum'}).reset_index()
    Total_population_by_country_age.iloc[:,1:].to_excel(f'{output_path_files}Q2. Population by country throughout the years.xlsx', index=False)

    #Query 3 (Population pyramid)
    Population_by_ages = Population_by_ages.iloc[:,:-1].merge(Total_population_by_country_age.loc[:,['ISO3_code','Time','PopTotal']], how='left', on=['ISO3_code', 'Time'])
    Population_by_ages.insert(7, "Percentage_PopMale", Population_by_ages['PopMale']/Population_by_ages['PopTotal'])
    Population_by_ages.insert(8, "Percentage_PopFemale", Population_by_ages['PopFemale']/Population_by_ages['PopTotal'])
    Population_by_ages.iloc[:,2:-1].to_excel(f'{output_path_files}Q3. Population pyramid.xlsx', index=False)

    #Query 4 (Human and social development indicators)
    Index_categories = pd.DataFrame({'hdi_category':Categorizing_index(Multiple_demo_index, 'hdi'),
                                     'mys_category':Categorizing_index(Multiple_demo_index, 'mys'),
                                     'gii_category':Categorizing_index(Multiple_demo_index, 'gii')})
    Multiple_demo_index = pd.concat([Multiple_demo_index, Index_categories], axis=1)
    Multiple_demo_index.iloc[:,1:].to_excel(f'{output_path_files}Q4. Human and social development indicators.xlsx', index=False)

    #Query 5 (Average hours worked by country and year)
    Hours_worked = Hours_worked.dropna()
    Hours_worked.dropna().iloc[:,1:].to_excel(f'{output_path_files}Q5. Avg hours worked by country.xlsx', index=False)

    #Query 6 (Average hours worked vs Multiple indicators)
    Indic_comparison = Hours_worked.merge(Demo_index.loc[:,['ISO3_code','Time','CBR']], how='inner', on=['ISO3_code','Time'])
    Indic_comparison = Indic_comparison.merge(Multiple_demo_index.loc[:,['ISO3_code','Time','hdi','mys']], how='inner', on=['ISO3_code','Time'])
    Indic_comparison.iloc[:,1:].to_excel(f'{output_path_files}Q6. Avg hours vs multiple indicators.xlsx', index=False)

    #Query 7 (Average hours worked of OECD countries)
    Hours_worked = Filtering_OECD_countries(Hours_worked)
    Hours_worked.iloc[:,1:].to_excel(f'{output_path_files}Q7.OECD countries hours worked.xlsx', index=False)