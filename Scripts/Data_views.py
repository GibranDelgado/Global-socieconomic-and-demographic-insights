import pandas as pd
import Development_and_sociodemographic_indicators as DSI

class DataImport:
    def __init__(self, inputPath):
        self.inputPath = inputPath
    
    def __get_sources(self):
        files = {
            'regions':'ISO3_codes_world_regions.csv',
            'demoIndex':'WPP2022_Demographic_Indicators_Medium.csv',
            'popByAges':'WPP2022_PopulationByAge5GroupSex_Medium.csv',
            'multDemoIndex':"HDR23-24_Composite_indices_complete_time_series.csv",
            'hoursWorked':'pwt1001.xlsx'
        }

        srcRegions = pd.read_csv(f"{self.inputPath}{files['regions']}",
                                 usecols=['alpha-3','name','region','sub-region'])    

        srcDemoIndex = pd.read_csv(f"{self.inputPath}{files['demoIndex']}", 
                                   usecols=['ISO3_code','Location','LocTypeName',
                                            'Time','CBR','LEx'])
        
        srcPopByAges = pd.read_csv(f"{self.inputPath}{files['popByAges']}",
                                   usecols=['ISO3_code','Location','LocTypeName',
                                            'Time','AgeGrp','PopMale','PopFemale', 
                                            'PopTotal'])
        
        srcMultDemoIndex = pd.read_csv(f"{self.inputPath}{files['multDemoIndex']}", 
                                       encoding='latin-1')
        
        srcHoursWorked = pd.read_excel(f"{self.inputPath}{files['hoursWorked']}", 
                                     sheet_name='Data', 
                                     usecols=['countrycode','country','year','avh'])

        return {
            'srcRegions':srcRegions,
            'srcDemoIndex':srcDemoIndex,
            'srcPopByAges':srcPopByAges,
            'srcMultDemoIndex':srcMultDemoIndex,
            'srcHoursWorked':srcHoursWorked
        }
    
    
    def get_dataframes(self):
        sources = self.__get_sources()
        
        regions = sources['srcRegions']
        demoIndex = DSI.DemoIndex(sources['srcDemoIndex'])
        popByAges = DSI.PopulationByAges(sources['srcPopByAges'])
        popByAges = popByAges.clean_population_by_ages()
        multDemoIndex = DSI.MultipleDemoIndex(sources['srcMultDemoIndex'], popByAges)
        hoursWorked = DSI.HoursWorked(sources['srcHoursWorked'])
        
        return {
            'regions':regions,
            'demoIndex':demoIndex.clean_demo_index(regions),
            'popByAges':popByAges,
            'multDemoIndex':multDemoIndex.clean_multiple_demo_index(regions),
            'hoursWorked':hoursWorked.clean_hours_worked()
        }

class UpdateDataFramesMethods:    
    def total_pop_by_country(self, popByAges):
        totalPop = popByAges.groupby(['ISO3_code','Official_names','Time'])
        return totalPop.agg({'PopTotal':'sum'}).reset_index()
        
    def __drop_column(self, popByAges):
        return popByAges.drop(columns=['PopTotal'])
    
    def __pop_gender_percentage(self, popByAges, gender):
        return popByAges[f'Pop{gender}']/popByAges['PopTotal']
        
    def __merge(self, df1, df2):
        return df1.merge(df2, how='inner')
    
    def population_pyramid(self, popByAges, totalCountryPop):
        popByAges = self.__drop_column(popByAges)
        popByAges = self.__merge(popByAges, totalCountryPop)
        popByAges['Percentage_PopMale'] = self.__pop_gender_percentage(popByAges, 'Male')
        popByAges['Percentage_PopFemale'] = self.__pop_gender_percentage(popByAges, 'Female')
        return self.__drop_column(popByAges)
    
    def metrics_comparison(self, hoursWorked, demoIndex, multDemoIndex):
        metricsComparison = self.__merge(hoursWorked, demoIndex[['ISO3_code','Time','CBR']])
        return self.__merge(metricsComparison, multDemoIndex[['ISO3_code','Time','hdi','mys']])
    
    def oecd_countries(self, hoursWorked):
        OECD = pd.DataFrame({'ISO3_code':('AUS','AUT','BEL','CAN','CHL','COL',
                                          'CRI','CZE','DNK','EST','FIN','FRA',
                                          'DEU','GRC','HUN','ISL','IRL','ISR',
                                          'ITA','JPN','KOR','LVA','LTU','LUX',
                                          'MEX','NLD','NZL','NOR','POL','PRT',
                                          'SVK','SVN','ESP','SWE','CHE','TUR',
                                          'GBR','USA')})
        return self.__merge(hoursWorked, OECD)

class CreateViews:
    def __init__(self, outputPath):
        self.outputPath = outputPath
    
    def __df_to_excel(self, df, name):
        print(f'\nGenerating file {name}...')
        df.iloc[:,1:].to_excel(f'{self.outputPath}{name}.xlsx', index=False)
        print('File generated!')
    
    def views_creating(self, DFs):
        methods = UpdateDataFramesMethods()
        
        demoIndex = DFs['demoIndex']
        popByAges = DFs['popByAges']
        multDemoIndex = DFs['multDemoIndex']
        hoursWorked = DFs['hoursWorked']
        
        filesNames = {
            'Q1':'Q1. World map demographic indicators',
            'Q2':'Q2. Population by country throughout the years',
            'Q3':'Q3. Population pyramid',
            'Q4':'Q4. Human and social development indicators',
            'Q5':'Q5. Avg hours worked by country',
            'Q6':'Q6. Avg hours vs multiple indicators.xlsx',
            'Q7':'Q7. OECD countries hours worked'
        }
        
        #Query 1 (Demographic index world map)
        self.__df_to_excel(demoIndex, filesNames['Q1'])

        #Query 2 (Country population throughout the years)
        totalCountryPop = methods.total_pop_by_country(popByAges)
        self.__df_to_excel(totalCountryPop, filesNames['Q2'])
        
        #Query 3 (Population pyramid)
        populationPyramid = methods.population_pyramid(popByAges, totalCountryPop)
        self.__df_to_excel(populationPyramid, filesNames['Q3'])
        
        #Query 4 (Human and social development indicators)
        self.__df_to_excel(multDemoIndex, filesNames['Q4'])
        
        #Query 5 (Average hours worked by country and year)
        self.__df_to_excel(hoursWorked, filesNames['Q5'])

        #Query 6 (Average hours worked vs Multiple indicators)
        metricsComparison = methods.metrics_comparison(hoursWorked, demoIndex, multDemoIndex)
        self.__df_to_excel(metricsComparison, filesNames['Q6'])

        #Query 7 (Average hours worked of OECD countries)
        hoursWorked = methods.oecd_countries(hoursWorked)
        self.__df_to_excel(hoursWorked, filesNames['Q7'])