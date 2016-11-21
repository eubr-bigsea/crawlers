# Web crawlers

This repository is composed of all web crawlers source codes implemented at UFMG for the Big Sea project. The data collected are divided in seven nature categories:

  - Events (Social network data)
  - Social (Social network data)
  - Mobility (Dynamic spatial data)
  - Traffic (Dynamic spatial data)
  - News (Stationary data)
  - Points of interest (Stationary data)
  - Weather (Environmental data)

Data can be acquired either by API queries or web scraping.

### Events crawlers

**Facebook:**Through the FB API, this crawler gets data about events on Facebook occurring in the 10 largest cities of Brazil (Belo Horizonte, Brasília, Curitiba, Fortaleza, Manaus, Porto Alegre, Recife, Rio de Janeiro, Salvador, São Paulo).
There is no way to find all events within these cities, therefore we apply a heuristic to find events as detailed following. First, we map the subdistricts of each city. These subdistricts are smaller regions inside the cities. Second, we map the centroid of each subdistrict. We use the IBGE database to perform these two steps. Third, for each centroid, we query for existing venues on Facebook located whithn 2000 meters radius from the centroid. Finally, we query for events created by these venues on Facebook.


**Globo:**Collects information about artistic events released at Globo website. 

**Google:**Collects data about artistic events suggested by Google in the top box that appears whenever a seach in a city for events is performed. 

#### Mobility crawlers

**Curitiba busposition:** 

**curitiba static:** 

**rio busposition1:** 

**rio busposition2:** 

**saopaulo buslines:** 


#### News crawlers

**news_curitiba_cityhall:** 

#### POI crawlers

**poi_curitiba:** 

**poi_tripadvisor:** 

#### Traffic crawlers

**traffic_maplink:** 


#### Weather crawlers

**weather_darksky:** 

**weather_openweather:** 

**weather_openweather_stations:** 



## Implementation details



#### Common methods

  - 
  - 
  - 

#### Common classes

**Control:** 
**DB:** 
**Monitoring:** 
