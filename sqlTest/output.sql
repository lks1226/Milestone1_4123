CREATE TABLE sqlOutput AS
WITH tempT as
	(SELECT SUBSTR(Timestamp,0, 8) month, station, MAX(humidity) maxH, MAX(temperature) maxT, MIN(humidity) minH, MIN(temperature) minT
		FROM SingaporeWeather_1
		WHERE (timestamp LIKE "2003%" OR timestamp LIKE "2013%") AND station = "Paya Lebar"
        GROUP BY month)
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", l.Station,
	"Min Humidity    " as Category, l.Humidity as value
	FROM SingaporeWeather_1 as l JOIN tempT as r on l.humidity = r.minH
    	AND SUBSTR(l.Timestamp,0, 8) = r.month AND l.Station = r.Station
UNION
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", l.Station,
	"Max Humidity    " as Category, l.Humidity as value
	FROM SingaporeWeather_1 as l JOIN tempT as r on l.humidity = r.maxH
    	AND SUBSTR(l.Timestamp,0, 8) = r.month AND l.Station = r.Station
UNION
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", l.Station,
	"Min Temperature" as Category, l.Temperature as value
	FROM SingaporeWeather_1 as l JOIN tempT as r on l.temperature = r.minT
    	AND SUBSTR(l.Timestamp,0, 8) = r.month AND l.Station = r.Station
UNION
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", l.Station,
	"Max Temperature" as Category, l.Temperature as value
	FROM SingaporeWeather_1 as l JOIN tempT as r on l.temperature = r.maxT
    	AND SUBSTR(l.Timestamp,0, 8) = r.month AND l.Station = r.Station
ORDER BY 2 DESC;