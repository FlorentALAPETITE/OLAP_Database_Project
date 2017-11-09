set lines 175;
set trimout on;
set tab off;
spool queryResult.txt




-- ROLLUP sur les incident des années, mois, états, villes
PROMPT Requete : GROUP BY ROLLUP sur les incident des annes, mois, etats, villes;


SELECT city,state,month,year, count(*) as nbIncident
FROM Fact NATURAL JOIN DimDate NATURAL JOIN DimPlace
GROUP BY ROLLUP(year,month,state,city); 


PROMPT  -----------------;
PROMPT ;




-- TOP 5 des villes avec le plus de meurtres sur toute la période de temps
PROMPT Requete : TOP 5 des villes avec le plus de meurtres sur toute la periode de temps;


SELECT * FROM (
	SELECT SUM(victimCount+1) as nbVictimes, city, state, year
	FROM Fact NATURAL JOIN DimDate NATURAL JOIN DimPlace	
	GROUP BY year, city, state
	ORDER BY sum(victimCount) DESC)
WHERE ROWNUM<=5;


PROMPT  -----------------;
PROMPT ;




-- CUBE sur les perpetrator 
PROMPT Requete : GROUP BY CUBE sur les auteurs de crimes;


SELECT perpetratorAge as age, perpetratorSex as sex, perpetratorRace as race, perpetratorEthnicity as ethnicity, SUM(victimCount+1) as nbVictimes
FROM Fact, DimProfile
WHERE perpetratorAge = DimProfile.age AND perpetratorSex = DimProfile.sex AND perpetratorRace = DimProfile.race AND perpetratorEthnicity = DimProfile.ethnicity
GROUP BY CUBE (perpetratorAge, perpetratorSex, perpetratorRace, perpetratorEthnicity);


PROMPT  -----------------;
PROMPT ;



-- WINDOW sur le nombre de victimes par mois
PROMPT Requete : WINDOW sur le nombre de victimes par mois;


SELECT year, month, sum(victimCount+1) as nbVictimes, sum(sum(victimCount+1)) over (order by month) as accumulationVictimes
FROM FACT natural join DimDate
GROUP BY year,month;



PROMPT  -----------------;
PROMPT ;




-- Liste les armes recensées dans les incidents et le nombre d'implication de celles-ci
PROMPT Requete : Liste les armes recensees dans les incidents et le nombre d implication de celles-ci;


SELECT weapon, sum(1+victimCount) as victimes
FROM Fact
GROUP BY weapon
ORDER BY victimes DESC;

PROMPT  -----------------;
PROMPT ;



-- GROUPING SET 
PROMPT Requete : GROUPING SET par : (etat, annee, sexeVictime), (annee, sexeVictime), (etat, sexeVictime), (sexeVictime);


SELECT Fact.year, Fact.state, Fact.victimSex, SUM(victimCount+1) as victimes
FROM Fact, DimProfile, DimDate
WHERE Fact.year = DimDate.year and Fact.month = DimDate.month and victimAge = DimProfile.age AND victimSex = DimProfile.sex AND victimRace = DimProfile.race AND victimEthnicity = DimProfile.ethnicity
GROUP BY GROUPING SETS ((Fact.state, Fact.year, Fact.victimSex), (Fact.year,Fact.victimSex), (Fact.state, Fact.victimSex), (Fact.victimSex));

PROMPT  -----------------;
PROMPT ;

-- GROUPING
PROMPT Requete : GROUPING + GROUP BY ROLLUP pour récupérer le nb de victimes par mois et par state ;


SELECT DimDate.month, DimPlace.state, sum(1+victimCount) as victims, GROUPING(DimDate.month) as monthB, GROUPING(DimPlace.state) as stateB
FROM Fact, DimDate, DimPlace
WHERE Fact.year = DimDate.year AND Fact.month = DimDate.month AND Fact.city = DimPlace.city AND Fact.state = DimPlace.state
GROUP BY ROLLUP(DimDate.month,DimPlace.state);

PROMPT  -----------------;
PROMPT ;

-- RANK
PROMPT Requete : RANK of victims by seasons;


SELECT season, sum(1+victimCount) as victims,
	RANK() OVER (ORDER BY sum(1+victimCount) DESC) as rank
FROM Fact, DimDate
WHERE Fact.year = DimDate.year AND Fact.month = DimDate.month
GROUP BY season;

PROMPT  -----------------;
PROMPT ;



-- RANK + PARTITION BY
PROMPT Requete : RANK of victims by seasons, for each years using PARTITION BY;


SELECT DimDate.year, DimDate.season, sum(1+victimCount) as victims,
	RANK() OVER (PARTITION BY DimDate.year ORDER BY sum(1+victimCount) DESC) as rank
FROM Fact, DimDate
WHERE Fact.year = DimDate.year AND Fact.month = DimDate.month
GROUP BY (DimDate.year,DimDate.season);

PROMPT  -----------------;
PROMPT ;

spool off;
