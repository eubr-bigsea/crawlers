import datetime

crawlers = [
"events_facebook.control",
"events_globo.control",
"events_google.control",
"mobility_curitiba_busposition.control",
"mobility_curitiba_static.control",
"mobility_rio_busposition1.control",
"mobility_rio_busposition2.control",
"news_curitiba_cityhall.control",
"poi_curitiba.control",
"traffic_maplink.control",
"weather_darksky.control",
"weather_openweather.control",
"weather_openweather_stations.control"
]

for crawler in crawlers:
    with open(crawler) as infile:
        data = infile.read()
    print datetime.datetime.fromtimestamp(float(data)), "\t\t\t", crawler
