import pandas as pd
import numpy as np

def Correcting_country_names(df):
    import pycountry as pyc
    
    Country_codes = pd.unique(df.ISO3_code)
    Country_names = pd.unique(df.Location)
    Correct_names = []
        
    for Code, Name in zip(Country_codes, Country_names):
        Result = pyc.countries.get(alpha_3=Code)

        if Result is not None: #If code is already asigned to a country
            Correct_names.append(Result.name)
        else:
            Correct_names.append(Name) #Otherwise just let the name as is
            
    
    Corrected_country_info = pd.DataFrame({'ISO3_code':Country_codes,'Official_names':Correct_names})  
    
    return df.merge(Corrected_country_info, how='inner', on='ISO3_code').iloc[:,:-1]

def Classifying_country_regions(Regions):
    Regions = Regions.rename(columns={'alpha-3':'ISO3_code','region':'continent'})
    New_subregions = pd.DataFrame({'sub-region':list(pd.unique(Regions['sub-region'])),
                                    'region':['Central and Southern Asia','Northern, Southern and Western Europe','Northern, Southern and Western Europe',
                                    'Middle East and North Africa','Polynesia','Sub-Saharan Africa','Northern and Central America and Caribbean','South America',
                                    'Middle East and North Africa','East Asia and Pacific','Northern, Southern and Western Europe','Eastern Europe',
                                    'Northern and Central America and Caribbean','Northern and Central America and Caribbean','East Asia and Pacific','East Asia and Pacific',
                                    'Melanesia','Micronesia','Central and Southern Asia'],
                                    'world':'World'})
    
    # NLAC = North & Latin America & Caribbean
    # EU = Europe
    # MENA = Middle East & North Africa
    # EAP = East Asia & Pacific
    # CA = Central Asia
    # SA = South Asia
    # SSA = Sub-Saharan Africa
    
    return Regions.merge(New_subregions, how='inner', on='sub-region')

def Cleaning_demo_index(Demo_index_src, Regions):
    #I will take only the countries, not regions or subregions
    Demo_index_stg = Demo_index_src[(Demo_index_src.LocTypeName=='Country/Area')&(Demo_index_src.Time<2100)]
    Demo_index_stg = Correcting_country_names(Demo_index_stg)
    Demo_index_stg['BirthRate_category'] = Demo_index_stg.groupby('Time')[['CBR']].transform(lambda x:pd.cut(x,5,labels=['Very low','Low','Medium','High','Very high'])).astype(str)
    Demo_index_stg['LifeExpectancy_category'] = Demo_index_stg.groupby('Time')[['LEx']].transform(lambda x:pd.cut(x,5,labels=['Very low','Low','Medium','High','Very high'])).astype(str)
    
    Regions = Classifying_country_regions(Regions)
    Demo_index_stg =  Demo_index_stg.merge(Regions.loc[:,['ISO3_code','region','continent','world']], how='inner', on='ISO3_code')
    
    return Demo_index_stg

def Cleaning_population_by_ages(Population_by_ages_src):
    Population_by_ages_stg = Population_by_ages_src[Population_by_ages_src.LocTypeName=='Country/Area']
    Population_by_ages_stg = pd.concat([Population_by_ages_stg.iloc[:,:5], Population_by_ages_stg.iloc[:,-3:]*1000], axis=1)    
    Population_by_ages_stg = Correcting_country_names(Population_by_ages_stg)
      
    return Population_by_ages_stg

def Cleaning_multiple_demo_index(Multiple_demo_index_src, Regions, Population_by_ages_stg):
    Multiple_demo_index_stg = Multiple_demo_index_src.rename(columns={'iso3':'ISO3_code', 'country':'Location'})
    #Rows 195 and the following are regions/subregions, so just ignore them
    Multiple_demo_index_stg = Correcting_country_names(Multiple_demo_index_stg.loc[:194,:])
    
    Regions = Classifying_country_regions(Regions)
    Country_info = Multiple_demo_index_stg.iloc[:194,0:2]
    Country_info =  Country_info.merge(Regions.loc[:,['ISO3_code','region','continent','world']], how='inner', on='ISO3_code')
    hdi = Multiple_demo_index_stg.iloc[:, 5:38] #These columns belong hdi index
    mys = Multiple_demo_index_stg.iloc[:, 104:137] #These columns belong mys index
    gii = Multiple_demo_index_stg.iloc[:, 613:646] #These columns belong gii index
    Times = tuple(range(1990,2023)) #Range of years between 1990 and 2022
    
    hdi_new = []
    mys_new = []
    gii_new = []
    Times_new = []
    
    for i in range(len(Country_info)):
        hdi_new.extend(hdi.iloc[i,:])
        mys_new.extend(mys.iloc[i,:])
        gii_new.extend(gii.iloc[i,:])
        Times_new.extend(Times)
    
    Country_info_new = pd.DataFrame(np.repeat(Country_info.values,len(Times),axis=0))
    Country_info_new.columns = Country_info.columns
    Multi_demo_index_values = pd.DataFrame({'Time':Times_new, 'hdi':hdi_new, 'mys':mys_new, 'gii':gii_new})
    Multiple_demo_index_stg = pd.concat([Country_info_new, Multi_demo_index_values], axis=1)
    Total_population_by_country_age = Population_by_ages_stg.groupby(['ISO3_code','Time']).agg({'PopTotal':'sum'}).reset_index()
    Multiple_demo_index_stg = Multiple_demo_index_stg.merge(Total_population_by_country_age, how='inner', on=['ISO3_code', 'Time'])
    
    return Multiple_demo_index_stg

def Cleaning_hours_worked(Hours_worked_src):
    Hours_worked_stg = Hours_worked_src.rename(columns={'countrycode':'ISO3_code', 'country':'Location', 'year':'Time', 'avh':'Avg_hours'})
    Hours_worked_stg = Correcting_country_names(Hours_worked_stg)
    
    return Hours_worked_stg