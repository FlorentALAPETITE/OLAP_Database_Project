DROP TABLE DimPlace CASCADE CONSTRAINTS;
DROP TABLE DimAgency CASCADE CONSTRAINTS;
DROP TABLE DimDate CASCADE CONSTRAINTS;
DROP TABLE DimPerson CASCADE CONSTRAINTS;
DROP SEQUENCE idPerson_seq;
DROP TABLE Fact CASCADE CONSTRAINTS;


CREATE TABLE DimPlace(
	city VARCHAR2(30) not null,
	state VARCHAR2(30) not null,
	CONSTRAINT dimPlace_pk PRIMARY KEY (city,state)
);

CREATE TABLE DimAgency(
	agencyCode VARCHAR2(7) not null,
	agencyName VARCHAR2(30) not null,
	agencyType VARCHAR2(30) not null,
	CONSTRAINT dimAgency_pk PRIMARY KEY (agencyCode)
);

CREATE TABLE DimDate(
	month VARCHAR2(20) not null,
	year NUMBER(4) not null,
	CONSTRAINT dimDate_pk PRIMARY KEY (month,year)
);

CREATE TABLE DimPerson(
	idPerson NUMBER not null,
	sex VARCHAR2(6) not null,
	age NUMBER(3) not null,
	race VARCHAR(60) not null,
	ethnicity VARCHAR(60) not null,
	CONSTRAINT dimPerson_pk PRIMARY KEY (idPerson)
);

CREATE SEQUENCE idPerson_seq
START WITH 1
INCREMENT BY 1;

CREATE OR REPLACE TRIGGER idPerson_tri 
BEFORE INSERT ON DimPerson 
FOR EACH ROW

BEGIN
  SELECT idPerson_seq.NEXTVAL
  INTO   :new.idPerson
  FROM   dual;
END;
/

CREATE TABLE Fact(
	agencyCode VARCHAR2(7) not null,
	month VARCHAR2(20) not null,
	year NUMBER not null,
	city VARCHAR2(30) not null,
	state VARCHAR2(30) not null,
	idVictim NUMBER not null,
	idMurderer NUMBER not null,
	crimeType VARCHAR2(30) not null,
	crimeSolved NUMBER(1,0) not null,
	relationship VARCHAR2(30) not null,
	weapon VARCHAR2(30) not null,
	recordSource VARCHAR2(10) not null,
	victimCount NUMBER not null,
	murdererCount NUMBER not null,
	incident NUMBER not null,
	CONSTRAINT fact_agency_fk FOREIGN KEY (agencyCode) REFERENCES DimAgency(agencyCode),
	CONSTRAINT fact_month_year_fk FOREIGN KEY (month,year) REFERENCES DimDate(month,year),
	CONSTRAINT fact_city_state_fk FOREIGN KEY (city,state) REFERENCES DimPlace(city,state),
	CONSTRAINT fact_victim_fk FOREIGN KEY (idVictim) REFERENCES DimPerson(idPerson),
	CONSTRAINT fact_murderer_fk FOREIGN KEY (idMurderer) REFERENCES DimPerson(idPerson),
	CONSTRAINT fact_pk PRIMARY KEY (agencyCode,month,year,city,idVictim,idMurderer)
);
