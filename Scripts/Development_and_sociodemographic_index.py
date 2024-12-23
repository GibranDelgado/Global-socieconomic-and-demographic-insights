import pandas as pd
import numpy as np
import pycountry as pyc

def concat(df1, df2):
    return pd.concat([df1, df2], axis=1)

def merge(df1, df2, on='ISO3_code'):
    return df1.merge(df2, how='inner', on=on)

class CorrectingCountriesInfo:
    def filter_the_regions(self, df):
        filteredDF = df[df.LocTypeName == 'Country/Area']
        return filteredDF.drop(columns=['LocTypeName'])

    def remove_redundant_years(self, df):
        return df[df.Time < 2100]

    def __get_unique_values(self, col):
        return pd.unique(col)

    def country_names(self, df):
        countryCodes = self.__get_unique_values(df.ISO3_code)
        countryNames = self.__get_unique_values(df.Location)
        correctNames = []

        for code, name in zip(countryCodes, countryNames):
            result = pyc.countries.get(alpha_3=code)
            name = result.name if result else name
            correctNames.append(name)

        correctedInfo = pd.DataFrame({
            'ISO3_code': countryCodes,
            'Official_names': correctNames
        })
        countriesInfo = merge(df, correctedInfo)
        return countriesInfo.drop(columns=['Location'])

class ClassifyCountryRegions:
    def __clean_the_data(self, regionsInfo):
        regionsInfo = regionsInfo.dropna()
        return regionsInfo.rename(columns={
            'alpha-3': 'ISO3_code',
            'region': 'continent'
        })
    
    def classify_subregions(self, regionsInfo):
        regionsInfo = self.__clean_the_data(regionsInfo)
        regions = {
            'Northern America': 'Northern and Central America and Caribbean',
            'Central America': 'Northern and Central America and Caribbean',
            'Caribbean': 'Northern and Central America and Caribbean',
            'South America': 'South America',

            'Northern Europe': 'Nothern, Southern and Western Europe',
            'Southern Europe': 'Nothern, Southern and Western Europe',
            'Western Europe': 'Nothern, Southern and Western Europe',
            'Eastern Europe': 'Eastern Europe',

            'Australia and New Zealand': 'East Asia and Pacific',
            'South-eastern Asia': 'East Asia and Pacific',
            'Eastern Asia': 'East Asia and Pacific',

            'Northern Africa': 'Middle East and North Africa',
            'Western Asia': 'Middle East and North Africa',
            'Sub-Saharan Africa': 'Sub-Saharan Africa',

            'Southern Asia': 'Central and Southern Asia',
            'Central Asia': 'Central and Southern Asia',

            'Micronesia': 'Micronesia',
            'Polynesia': 'Polynesia',
            'Melanesia': 'Melanesia'
        }
        regionsInfo['region'] = [regions[i] for i in regionsInfo['sub-region']]
        regionsInfo['world'] = 'World'
        return regionsInfo
    
    def merge_the_info(self, df, regionsInfo):
        col = ['ISO3_code','region','continent','world']
        return merge(df, regionsInfo.loc[:,col])
    
class DemoIndex(CorrectingCountriesInfo, ClassifyCountryRegions):
    def __init__(self, df):
        self.df = df
        
    def __classify_info(self, infoByYear):
        labels = ['Very low', 'Low', 'Medium', 'High', 'Very high']
        return pd.cut(infoByYear, bins=len(labels), labels=labels)
    
    def __metric_by_year(self, df, metric):
        infoByYear = df.groupby('Time')[[metric]]
        return infoByYear.transform(lambda x:self.__classify_info(x))

    def clean_demo_index(self, regions):
        demoIndex = self.filter_the_regions(self.df)
        demoIndex = self.remove_redundant_years(demoIndex)
        demoIndex = self.country_names(demoIndex)
        demoIndex['BirthRate_category'] = self.__metric_by_year(demoIndex, 'CBR')
        demoIndex['LifeExpectancy_category'] = self.__metric_by_year(demoIndex, 'LEx')
        regions = self.classify_subregions(regions)

        return self.merge_the_info(demoIndex, regions)

class PopulationByAges(CorrectingCountriesInfo):
    def __init__(self, df):
        self.df = df
    
    def clean_population_by_ages(self):
        popByAges = self.filter_the_regions(self.df)
        popByAges.loc[:,'PopMale':'PopTotal'] *= 1000
        return self.country_names(popByAges)

class MultipleDemoIndex(CorrectingCountriesInfo, ClassifyCountryRegions):
    def __init__(self, df, popByAges):
        self.df = df
        self.popByAges = popByAges
        self.start = 1990
        self.end = 2022
    
    def __clean_the_data(self, multipleDemoIndex):
        return multipleDemoIndex.rename(columns={
            'iso3':'ISO3_code',
            'country':'Location'
        })
    
    def __just_countries(self, multDemoIndex):
        mapping = [len(iso3)==3 for iso3 in multDemoIndex['ISO3_code']]
        return multDemoIndex[mapping]

    def __repeat_countryInfo(self, df):
        size = self.end - self.start + 1
        countryInfo = pd.DataFrame(np.repeat(df.values, size, axis=0))
        countryInfo.columns = df.columns
        return countryInfo
    
    def __metric_values(self, multDemoIndex, metric):
        start = f'{metric}_{self.start}'
        end = f'{metric}_{self.end}'
        metricsValues = multDemoIndex.loc[:, start:end]
        metricsValues = metricsValues.transpose().unstack().reset_index(drop=True)
        return pd.DataFrame(metricsValues, columns=[metric])
    
    def __repeat_years(self, multDemoIndex):
        numberOfCountries = multDemoIndex.shape[0]
        years = np.tile(range(self.start, self.end+1), numberOfCountries)
        return pd.DataFrame(pd.Series(years, name='Time'))
    
    def __mult_metric_values(self, multDemoIndex, metrics):
        multMetrics = None
        for i in metrics:
            metricsValues = self.__metric_values(multDemoIndex, i)
            multMetrics = concat(multMetrics, metricsValues)        
        years = self.__repeat_years(multDemoIndex)
        return concat(years, multMetrics)
    
    def __metric_categories(self, multDemoIndex, metric):
        metric_values = {
            'hdi':(0.4, 0.6, 0.75, 0.9),
            'gii':(0.15, 0.3, 0.5, 0.65),
            'mys':(2, 5, 8, 11)
        }
        labels = ['Very low','Low','Medium','High','Very high']
        categories = pd.cut(multDemoIndex[metric],
                            bins = (0,) + metric_values[metric] + (np.inf,),
                            labels=labels)
        return np.where(categories.isnull(), 'No data', categories)
    
    def __add_mult_metric_categories(self, multDemoIndex, metrics):
        for i in metrics:
            metricsCategories = self.__metric_categories(multDemoIndex, i)
            multDemoIndex[f'{i}_category'] = metricsCategories
    
    def __pop_by_country_age(self):
        popByCountryAge = self.popByAges.groupby(['ISO3_code','Time'])
        return popByCountryAge.agg({'PopTotal':'sum'}).reset_index()
                
    def clean_multiple_demo_index(self, regions):
        multDemoIndex = self.__clean_the_data(self.df)
        multDemoIndex = self.country_names(multDemoIndex)
        multDemoIndex = self.__just_countries(multDemoIndex)
        
        regions = self.classify_subregions(regions)
        countryInfo = multDemoIndex[['ISO3_code','Official_names']]
        countryInfo = self.merge_the_info(countryInfo, regions)
        countryInfo = self.__repeat_countryInfo(countryInfo)
        
        multMetrics = self.__mult_metric_values(multDemoIndex, ['hdi','mys','gii'])
        metricsByCountry = concat(countryInfo, multMetrics)
        self.__add_mult_metric_categories(metricsByCountry, ('hdi','mys','gii'))
        popByCountryAge = self.__pop_by_country_age()
        
        return merge(metricsByCountry, popByCountryAge, on=['ISO3_code', 'Time'])    

class HoursWorked(CorrectingCountriesInfo):
    def __init__(self, df):
        self.df = df
    
    def __clean_the_data(self):
        self.df = self.df.dropna()
        return self.df.rename(columns={
            'countrycode':'ISO3_code',
            'country':'Location',
            'year':'Time',
            'avh':'Avg_hours'
        })
    
    def oecd_countries(self, df):
        OECD = pd.DataFrame({'ISO3_code':('AUS','AUT','BEL','CAN','CHL','COL',
                                          'CRI','CZE','DNK','EST','FIN','FRA',
                                          'DEU','GRC','HUN','ISL','IRL','ISR',
                                          'ITA','JPN','KOR','LVA','LTU','LUX',
                                          'MEX','NLD','NZL','NOR','POL','PRT',
                                          'SVK','SVN','ESP','SWE','CHE','TUR',
                                          'GBR','USA')})
        return df.merge(OECD, how='inner', on='ISO3_code')
    
    def clean_hours_worked(self):
        hoursWorked = self.__clean_the_data()
        return self.country_names(hoursWorked)