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


**Globo:**artistic events in Brazil released by Globo website (web scraping).

**Google:**artistic events suggested by Google in the top box that appears whenever a seach in a city for events is performed (web scraping). 

### Mobility crawlers

**Curitiba Busposition:**Collects the geographic position of each bus vehicle in Curitiba. It uses the URBS (Curitiba public transportation company) API.

**Curitiba Static:**Static information about Curitiba bus: schedules, routes, paths, stops. Also uses the URBS API.

**Rio Busposition 1:**Buses' geographic location in Rio de Janeiro from the Data Rio webserver.

**Rio Busposition 2:**Buses' geographic location in Rio de Janeiro from the Data Rio webserver (different URL from the previous crawler)

**Sao Paulo Buslines:**Existing bus lines in Sao Paulo. It uses the SPTrans (Sao Paulo public transportation company) API.


### News crawlers

**News Curitiba Cityhall:**News released by the Curitiba City Hall about the treffic in the city.

### POI crawlers

**POI Curitiba:**data about the POI of Curitiba. It includes information about hospitals, hotels, schools, cemitery and pois of services for the population. The dataset is provided by the public transportation company of Curitiba, URBS through an API.

**POI Tripadvisor:**POI, facilities, pubs in the 10 most populated cities In Brazil from the Trip Advisor WebSite.

### Traffic crawlers

**Traffic Maplink:**traffic status in the main streets and avenues in the top ten cities in Brazil from the MapLink website.


### Weather crawlers

**Weather Darksky:**current weather and weather predictions for the top 10 Brazilian cities. The data is provided through the Darksky API and the target geographic points in the query are the subdistricts centroids of each city.

**Weather Openweather:**current weather in the top ten cities according to the Openweather API. It also considers the centroids of the cities' subdistricts. 

**Weather Openweather Stations:**current weather in the weather stations closest to the centroids of the cities' subdistricts.



## Implementation details



#### Common methods

  - 
  - 
  - 

#### Common classes

**Control:** 
**DB:** 
**Monitoring:** 
