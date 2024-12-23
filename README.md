# Global socioeconomic and demographic insights
Thank you for your interest in my project. This exploratory data analysis was made with the intention to see the evolution throughout the years of demographic indicators, such as birth rate (BR) and life expectancty (LE); and socio-demographic indicators, such as gender inequality index (GII), human development index (HDI) and mean years of schooling (MYS), for different regions of the world. Also, annual hours worked were by country were used to find a relation with the indicators mentioned before and seeing an trying to answer if too many hours of work has a negatively impact on them.

## Folders
The project only has two folders:
  - Data Sources: There you will find the source files used in this project.
  - Scripts: Contains two python scripts.
    1. "Development_and_sociodemographic_indicators": Contains many methods to clean and organize the data provided by source files. 
    2. "Data_views": Imports the source files required, use the methods detailed in the script mentioned before and generate the files needed to create the dashboard.

## How to use
Execute the "Main.py" script if you want to get the elements to recreate the dashboard. This going to create a new folder "Resulting files" with resulting excel files. 

## Libraries
  - pandas, numpy (if you are working in anaconda spyder you won't need to install them; otherwise probably you have to)
  - pycountry (probably you will need to install it)
  - sys, os (standar python libraries, no need to install)

## References
  - Birth rate and population: https://population.un.org/wpp/Download/Standard/MostUsed/  
  - Laboral hours worked: https://www.rug.nl/ggdc/productivity/pwt/  
  - HDI and other metrics: https://hdr.undp.org/data-center/documentation-and-downloads
  - OCDE countries = https://github.com/openclimatedata/countrygroups/blob/main/data/oecd.csv
