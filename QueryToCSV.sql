host mkdir -p CSVQueryResults

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 3
set numwidth 5

spool CSVQueryResults/QueryResult1.csv

SELECT city,state,month,year, count(*) as nbIncident
FROM admi2.Fact NATURAL JOIN admi2.DimDate NATURAL JOIN admi2.DimPlace
GROUP BY ROLLUP(year,month,state,city); 

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 4
set numwidth 5

spool CSVQueryResults/QueryResult2.csv

SELECT * FROM (
	SELECT SUM(victimCount+1) as nbVictimes, city, state, year
	FROM admi2.Fact NATURAL JOIN admi2.DimDate NATURAL JOIN admi2.DimPlace	
	GROUP BY year, city, state
	ORDER BY sum(victimCount) DESC)
WHERE ROWNUM<=5;

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 5
set numwidth 5

spool CSVQueryResults/QueryResult3.csv

SELECT perpetratorAge as age, perpetratorSex as sex, perpetratorRace as race, perpetratorEthnicity as ethnicity, SUM(victimCount+1) as nbVictimes
FROM admi2.Fact, DimProfile
WHERE perpetratorAge = DimProfile.age AND perpetratorSex = DimProfile.sex AND perpetratorRace = DimProfile.race AND perpetratorEthnicity = DimProfile.ethnicity
GROUP BY CUBE (perpetratorAge, perpetratorSex, perpetratorRace, perpetratorEthnicity);

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 3
set numwidth 5

spool CSVQueryResults/QueryResult4.csv

SELECT year, month, sum(victimCount+1) as nbVictimes, sum(sum(victimCount+1)) over (order by month) as accumulationVictimes
FROM admi2.FACT natural join admi2.DimDate
GROUP BY year,month;

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 2
set numwidth 5

spool CSVQueryResults/QueryResult5.csv

SELECT weapon, sum(1+victimCount) as victimes
FROM admi2.Fact
GROUP BY weapon
ORDER BY victimes DESC;

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 4
set numwidth 5

spool CSVQueryResults/QueryResult6.csv

SELECT admi2.Fact.year, admi2.Fact.state, admi2.Fact.victimSex, SUM(victimCount+1) as victimes
FROM admi2.Fact, DimProfile, admi2.DimDate
WHERE admi2.Fact.year = admi2.DimDate.year and admi2.Fact.month = admi2.DimDate.month and victimAge = DimProfile.age AND victimSex = DimProfile.sex AND victimRace = DimProfile.race AND victimEthnicity = DimProfile.ethnicity
GROUP BY GROUPING SETS ((admi2.Fact.state, admi2.Fact.year, admi2.Fact.victimSex), (admi2.Fact.year,admi2.Fact.victimSex), (admi2.Fact.state, admi2.Fact.victimSex), (admi2.Fact.victimSex));

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 4
set numwidth 5

spool CSVQueryResults/QueryResult7.csv

SELECT admi2.DimDate.month, admi2.DimPlace.state, sum(1+victimCount) as victims, GROUPING(admi2.DimDate.month) as monthB, GROUPING(admi2.DimPlace.state) as stateB
FROM admi2.Fact, admi2.DimDate, admi2.DimPlace
WHERE admi2.Fact.year = admi2.DimDate.year AND admi2.Fact.month = admi2.DimDate.month AND admi2.Fact.city = admi2.DimPlace.city AND admi2.Fact.state = admi2.DimPlace.state
GROUP BY ROLLUP(admi2.DimDate.month,admi2.DimPlace.state);

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 2
set numwidth 5

spool CSVQueryResults/QueryResult8.csv

SELECT season, sum(1+victimCount) as victims,
	RANK() OVER (ORDER BY sum(1+victimCount) DESC) as rank
FROM admi2.Fact, admi2.DimDate
WHERE admi2.Fact.year = admi2.DimDate.year AND admi2.Fact.month = admi2.DimDate.month
GROUP BY season;

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 3
set numwidth 5

spool CSVQueryResults/QueryResult9.csv

SELECT admi2.DimDate.year, admi2.DimDate.season, sum(1+victimCount) as victims,
	RANK() OVER (PARTITION BY admi2.DimDate.year ORDER BY sum(1+victimCount) DESC) as rank
FROM admi2.Fact, admi2.DimDate
WHERE admi2.Fact.year = admi2.DimDate.year AND admi2.Fact.month = admi2.DimDate.month
GROUP BY (admi2.DimDate.year,admi2.DimDate.season);

spool off;

set colsep ,
set headsep off
set pagesize 0
set trimspool on
set linesize 2
set numwidth 5

spool CSVQueryResults/QueryResult9.csv

SELECT agencyType, COUNT(crimeSolved) as nbCrimeSolved, NTILE(4) over(order by COUNT(crimeSolved) desc) as quarter
FROM admi2.Fact, admi2.DimAgency
WHERE admi2.Fact.agencyCode = admi2.DimAgency.agencyCode AND crimeSolved = 'Yes'
GROUP BY agencyType;

spool off;

