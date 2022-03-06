SELECT * FROM ScanResult as l
    LEFT JOIN sqlOutput sO on l.Date = sO.Date
        AND rtrim(l.Category) = rtrim(sO.Category) AND l.value = sO.value;