DROP TABLE DimPlace CASCADE CONSTRAINTS;
DROP TABLE DimAgency CASCADE CONSTRAINTS;
DROP TABLE DimDate CASCADE CONSTRAINTS;
DROP TABLE DimProfile CASCADE CONSTRAINTS;
DROP TABLE Fact CASCADE CONSTRAINTS;


CREATE TABLE DimPlace(
	city VARCHAR2(30) not null,
	state VARCHAR2(30) not null,
	CONSTRAINT dimPlace_pk PRIMARY KEY (city,state)
);

CREATE TABLE DimAgency(
	agencyCode VARCHAR2(7) not null,
	agencyName VARCHAR2(50) not null,
	agencyType VARCHAR2(30) not null,
	CONSTRAINT dimAgency_pk PRIMARY KEY (agencyCode)
);

CREATE TABLE DimDate(
	month VARCHAR2(20) not null,
	year NUMBER(4) not null,
	season VARCHAR2(20),
	CONSTRAINT dimDate_pk PRIMARY KEY (month,year)
);

CREATE TABLE DimProfile(
	sex VARCHAR2(7) not null,
	age NUMBER(3) not null,
	race VARCHAR(60) not null,
	ethnicity VARCHAR(60) not null,
	CONSTRAINT dimProfile_pk PRIMARY KEY (sex, age, race, ethnicity)
);


CREATE TABLE Fact(
	idCrime NUMBER not null,
	agencyCode VARCHAR2(7) not null,
	month VARCHAR2(20) not null,
	year NUMBER(4) not null,
	city VARCHAR2(30) not null,
	state VARCHAR2(30) not null,	
	crimeType VARCHAR2(30) not null,
	crimeSolved VARCHAR2(3) not null,
	relationship VARCHAR2(30) not null,
	weapon VARCHAR2(30) not null,
	recordSource VARCHAR2(10) not null,
	victimCount NUMBER not null,
	perpetratorCount NUMBER not null,
	incident NUMBER not null,
	victimSex VARCHAR2(7) not null,
	victimAge NUMBER(3) not null,
	victimRace VARCHAR(60) not null,
	victimEthnicity VARCHAR(60) not null,
	perpetratorSex VARCHAR2(7) not null,
	perpetratorAge NUMBER(3) not null,
	perpetratorRace VARCHAR(60) not null,
	perpetratorEthnicity VARCHAR(60) not null,
	CONSTRAINT fact_agency_fk FOREIGN KEY (agencyCode) REFERENCES DimAgency(agencyCode),
	CONSTRAINT fact_month_year_fk FOREIGN KEY (month,year) REFERENCES DimDate(month,year),
	CONSTRAINT fact_city_state_fk FOREIGN KEY (city,state) REFERENCES DimPlace(city,state),
	CONSTRAINT fact_victim_fk FOREIGN KEY (victimSex,victimAge,victimRace,victimEthnicity) REFERENCES DimProfile(sex,age,race,ethnicity),
	CONSTRAINT fact_murderer_fk FOREIGN KEY (perpetratorSex,perpetratorAge,perpetratorRace,perpetratorEthnicity) REFERENCES DimProfile(sex,age,race,ethnicity),
	CONSTRAINT fact_pk PRIMARY KEY (idCrime)
);


GRANT ALL ON DimPlace TO admi45;
GRANT ALL ON DimProfile TO admi45; 
GRANT ALL ON DimDate TO admi45;
GRANT ALL ON DimAgency TO admi45; 
GRANT ALL ON Fact TO admi45;


quit;