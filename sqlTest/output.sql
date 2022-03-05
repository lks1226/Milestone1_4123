CREATE TABLE sqlOutput AS 
WITH tempT as
	(SELECT SUBSTR(Timestamp,0, 8) month, MAX(humidity) maxH, MAX(temperature) maxT, MIN(humidity) minH, MIN(temperature) minT
		FROM year02_1_31
		WHERE timestamp LIKE "2002%" AND station = "Changi"
        GROUP BY month)
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", 
	"Min Humidity    " as Category, l.Humidity as value
	FROM year02_1_31 as l JOIN tempT as r on l.humidity = r.minH 
    	AND SUBSTR(l.Timestamp,0, 8) = r.month
UNION
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", 
	"Max Humidity    " as Category, l.Humidity as value
	FROM year02_1_31 as l JOIN tempT as r on l.humidity = r.maxH
    	AND SUBSTR(l.Timestamp,0, 8) = r.month
UNION
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", 
	"Min Temperature" as Category, l.Temperature as value
	FROM year02_1_31 as l JOIN tempT as r on l.temperature = r.minT
    	AND SUBSTR(l.Timestamp,0, 8) = r.month
UNION
SELECT DISTINCT(SUBSTR(l.Timestamp,0, 11)) as "Date", 
	"Max Temperature" as Category, l.Temperature as value
	FROM year02_1_31 as l JOIN tempT as r on l.temperature = r.maxT
    	AND SUBSTR(l.Timestamp,0, 8) = r.month
ORDER BY 2 DESC;