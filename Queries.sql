set lines 175;
set trimout on;
set tab off;
spool queryResult.txt




-- ROLLUP sur les incident des années, mois, états, villes
PROMPT Requete : GROUP BY ROLLUP sur les incident des annes, mois, etats, villes;


SELECT city,state,month,year, count(*) as nbIncident
FROM admi2.Fact NATURAL JOIN admi2.DimDate NATURAL JOIN admi2.DimPlace
GROUP BY ROLLUP(year,month,state,city); 


PROMPT  -----------------;
PROMPT ;




-- TOP 5 des villes avec le plus de meurtres sur toute la période de temps
PROMPT Requete : TOP 5 des villes avec le plus de meurtres sur toute la periode de temps;


SELECT * FROM (
	SELECT SUM(victimCount+1) as nbVictimes, city, state, year
	FROM admi2.Fact NATURAL JOIN admi2.DimDate NATURAL JOIN admi2.DimPlace	
	GROUP BY year, city, state
	ORDER BY sum(victimCount) DESC)
WHERE ROWNUM<=5;


PROMPT  -----------------;
PROMPT ;




-- CUBE sur les perpetrator 
PROMPT Requete : GROUP BY CUBE sur les auteurs de crimes;


SELECT perpetratorAge as age, perpetratorSex as sex, perpetratorRace as race, perpetratorEthnicity as ethnicity, SUM(victimCount+1) as nbVictimes
FROM admi2.Fact, DimProfile
WHERE perpetratorAge = DimProfile.age AND perpetratorSex = DimProfile.sex AND perpetratorRace = DimProfile.race AND perpetratorEthnicity = DimProfile.ethnicity
GROUP BY CUBE (perpetratorAge, perpetratorSex, perpetratorRace, perpetratorEthnicity);


PROMPT  -----------------;
PROMPT ;



-- WINDOW sur le nombre de victimes par mois
PROMPT Requete : WINDOW sur le nombre de victimes par mois;


SELECT year, month, sum(victimCount+1) as nbVictimes, sum(sum(victimCount+1)) over (order by month) as accumulationVictimes
FROM admi2.FACT natural join admi2.DimDate
GROUP BY year,month;



PROMPT  -----------------;
PROMPT ;




-- Liste les armes recensées dans les incidents et le nombre d'implication de celles-ci
PROMPT Requete : Liste les armes recensees dans les incidents et le nombre d implication de celles-ci;


SELECT weapon, sum(1+victimCount) as victimes
FROM admi2.Fact
GROUP BY weapon
ORDER BY victimes DESC;

PROMPT  -----------------;
PROMPT ;



-- GROUPING SET 
PROMPT Requete : GROUPING SET par : (etat, annee, sexeVictime), (annee, sexeVictime), (etat, sexeVictime), (sexeVictime);


SELECT admi2.Fact.year, admi2.Fact.state, admi2.Fact.victimSex, SUM(victimCount+1) as victimes
FROM admi2.Fact, DimProfile, admi2.DimDate
WHERE admi2.Fact.year = admi2.DimDate.year and admi2.Fact.month = admi2.DimDate.month and victimAge = DimProfile.age AND victimSex = DimProfile.sex AND victimRace = DimProfile.race AND victimEthnicity = DimProfile.ethnicity
GROUP BY GROUPING SETS ((admi2.Fact.state, admi2.Fact.year, admi2.Fact.victimSex), (admi2.Fact.year,admi2.Fact.victimSex), (admi2.Fact.state, admi2.Fact.victimSex), (admi2.Fact.victimSex));

PROMPT  -----------------;
PROMPT ;



-- GROUPING
PROMPT Requete : GROUPING + GROUP BY ROLLUP pour récupérer le nombre de victimes par mois et par états;


SELECT admi2.DimDate.month, admi2.DimPlace.state, sum(1+victimCount) as victims, GROUPING(admi2.DimDate.month) as monthB, GROUPING(admi2.DimPlace.state) as stateB
FROM admi2.Fact, admi2.DimDate, admi2.DimPlace
WHERE admi2.Fact.year = admi2.DimDate.year AND admi2.Fact.month = admi2.DimDate.month AND admi2.Fact.city = admi2.DimPlace.city AND admi2.Fact.state = admi2.DimPlace.state
GROUP BY ROLLUP(admi2.DimDate.month,admi2.DimPlace.state);

PROMPT  -----------------;
PROMPT ;



-- RANK
PROMPT Requete : RANK des victimes par saisons;


SELECT season, sum(1+victimCount) as victims,
	RANK() OVER (ORDER BY sum(1+victimCount) DESC) as rank
FROM admi2.Fact, admi2.DimDate
WHERE admi2.Fact.year = admi2.DimDate.year AND admi2.Fact.month = admi2.DimDate.month
GROUP BY season;

PROMPT  -----------------;
PROMPT ;



-- RANK + PARTITION BY
PROMPT Requete : RANK des victimes par saisons, pour chaque annee avec PARTITION BY;


SELECT admi2.DimDate.year, admi2.DimDate.season, sum(1+victimCount) as victims,
	RANK() OVER (PARTITION BY admi2.DimDate.year ORDER BY sum(1+victimCount) DESC) as rank
FROM admi2.Fact, admi2.DimDate
WHERE admi2.Fact.year = admi2.DimDate.year AND admi2.Fact.month = admi2.DimDate.month
GROUP BY (admi2.DimDate.year,admi2.DimDate.season);

PROMPT  -----------------;
PROMPT ;



-- NTILE
PROMPT Requete : Le type des agences et le nombre de crimes résolus par celles-ci, ordonnés par quart via NTILE;


SELECT agencyType, COUNT(crimeSolved) as nbCrimeSolved, NTILE(4) over(order by COUNT(crimeSolved) desc) as quarter
FROM admi2.Fact, admi2.DimAgency
WHERE admi2.Fact.agencyCode = admi2.DimAgency.agencyCode AND crimeSolved = 'Yes'
GROUP BY agencyType;

PROMPT  -----------------;
PROMPT ;

spool off;
