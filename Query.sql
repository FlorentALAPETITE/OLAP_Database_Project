
-- On verra ROLLUP

SELECT  city, state, month, year, victimCount
FROM Fact NATURAL JOIN DimDate NATURAL JOIN DimPlace
GROUP BY ROLLUP(city,state,month,year); 

-- TOP 10 des villes avec le plus de meutres sur toute la période de temps

SELECT SUM(victimCount), city, year
FROM Fact NATURAL JOIN DimDate NATURAL JOIN DimPlace
WHERE ROWNUM<=10
ORDER BY sum(victimCount) DESC
GROUP BY year, city;

-- 
