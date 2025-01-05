#!/usr/bin/env -S duckdb --no-stdin -init 
INSTALL iceberg;
LOAD iceberg;

-- Warmest temperatures
SELECT 
    airport_code as "Airport",
    date::date as "Date",
    ROUND(celsius, 1) || '°C' as "Temperature"
FROM iceberg_scan('warehouse/temps.db/temperatures')
ORDER BY celsius DESC
LIMIT 5;

-- Coldest temperatures
SELECT 
    airport_code as "Airport",
    date::date as "Date",
    ROUND(celsius, 1) || '°C' as "Temperature"
FROM iceberg_scan('warehouse/temps.db/temperatures')
ORDER BY celsius ASC
LIMIT 5;

.exit