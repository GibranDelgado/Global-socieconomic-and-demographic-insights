# Global socioeconomic and demographic insights
Thank you for your interest in my project. This exploratory data analysis was made with the intention to see the evolution throughout the years of demographic indicators, such as birth rate (BR) and life expectancty (LE); and socio-demographic indicators, such as gender inequality index (GII), human development index (HDI) and mean years of schooling (MYS), for different regions of the world. 

Also, annual hours worked were by country were used to find a relation with the indicators mentioned before and seeing an trying to answer if too many hours of work has a negatively impact on them.

## Content
The project has two folders:
  - **Data Sources**: There you will find the source files used in this project.
  - **Scripts**: Contains two python scripts.
      - **Development_and_sociodemographic_indicators**: Contains many methods to clean and organize the data provided by source files. 
      - **Data_views**: Imports the source files required, use the methods detailed in the script mentioned before and generate the files needed to create the dashboard.

## About the use
Execute the "Main.py" script. This going to create a new folder "Resulting files" with the excel files required to recreate the dashboard.

`Take into consideration that code processes thousands of registers, so maybe the execution time can reach the 9 minutes.`

## Dashboard
See the links below to visualize the implemented dashboard
  - NovyPro: https://project.novypro.com/jv1591
  - Github pages: https://gibrandelgado.github.io/Global-socieconomic-and-demographic-insights/

## Results discussion
  - **Birth Rate and life expectancy**: Both indicators are higher in Africa and Europe, respectively, than other regions of the world. However, while birth rate tends to decrease over the years, life expecancy tends to increase.
  - **Population trends:** In most of the countries, especially in first world countries, population tends to decrease each year and the population pyramid tends to invert. These factors affect the effective generational change.
  - **Socio-demogrpahic indicators:** Countries with a high humand development index and a high mean years of schooling also have a low gender inequality index and viceversa.
  - **Low birth rate countries categorization:** 
      - Those where people have high schooling, high life quality and fewer annual hours worked than the rest of the world decide their personal enjoyment over having kids.
      - Those where people only earn enough money to themselves due to low life quality, poor education and high annual hours worked, so they can't afford bringing new lifes to this world.

## Libraries
These three libraries were used. If you are working in an anaconda environment, probably you will not need the first two.
```
pip install pandas
pip install numpy
pip install pycountry
```

## References
  - Birth rate and population: https://population.un.org/wpp/Download/Standard/MostUsed/  
  - Laboral hours worked: https://www.rug.nl/ggdc/productivity/pwt/  
  - HDI and other metrics: https://hdr.undp.org/data-center/documentation-and-downloads
  - OCDE countries = https://github.com/openclimatedata/countrygroups/blob/main/data/oecd.csv
