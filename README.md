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

`Take in consideration that code are processing thousands of registers, so maybe the execution time can reach around the 9 minutes.`

## Dashboard

See the links below to visualize the implemented dashboard
  - NovyPro: https://project.novypro.com/jv1591
  - Github pages: https://gibrandelgado.github.io/Global-socieconomic-and-demographic-insights/

## Libraries
You will need to install these libraries. If you are working with anaconda, probably you will not need the first two
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
