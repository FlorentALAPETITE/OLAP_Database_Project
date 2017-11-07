set linesize 150;

-- ROLLUP sur les incident des villes, états, mois, années 

SELECT city,state,month,year, count(*)
FROM Fact NATURAL JOIN DimDate NATURAL JOIN DimPlace
GROUP BY ROLLUP(year,month,state,city); 


-- TOP 5 des villes avec le plus de meutres sur toute la période de temps
SELECT * FROM (
	SELECT SUM(victimCount), city, state, year
	FROM Fact NATURAL JOIN DimDate NATURAL JOIN DimPlace	
	GROUP BY year, city, state
	ORDER BY sum(victimCount) DESC)
WHERE ROWNUM<=5;

