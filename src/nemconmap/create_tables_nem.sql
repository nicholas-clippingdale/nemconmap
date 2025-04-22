CREATE SCHEMA IF NOT EXISTS nem;


CREATE TABLE IF NOT EXISTS nem.gencondata(
	 effectivedate DATE
	,versionno NUMERIC(3,0)
	,genconid VARCHAR(20)
	,constrainttype VARCHAR(2)
	,constraintvalue NUMERIC(16,6)
	,description VARCHAR(256)
	,genericconstraintweight NUMERIC(16,6)
	,lastchanged DATE
	,DISPATCH VARCHAR(1)
	,predispatch VARCHAR(1)
	,stpasa VARCHAR(1)
	,mtpasa VARCHAR(1)
	,limittype VARCHAR(64)
	,reason VARCHAR(256)
	,upload DATE
	
	,PRIMARY KEY (effectivedate, versionno, genconid)
);

--CREATE INDEX IDX_LASTCHANGED
--ON nem.con_info (LASTCHANGED);


CREATE TABLE IF NOT EXISTS nem.genconcoef(
	 termtype VARCHAR(20)
	,objectid VARCHAR(12)
	,effectivedate DATE
	,versionno NUMERIC(3,0)
	,genconid VARCHAR(20)
	,factor NUMERIC(16,6)
	,lastchanged DATE
	,bidtype VARCHAR(12)
	,upload DATE

	,PRIMARY KEY (termtype, objectid, effectivedate, genconid, versionno, bidtype)
);

--CREATE INDEX IDX_LASTCHANGED
--ON nem.con_coef (LASTCHANGED);


CREATE TABLE IF NOT EXISTS nem.dudetail(
	 effectivedate DATE
	,duid VARCHAR(10)
	,versionno NUMERIC(3,0)
	,connectionpointid VARCHAR(10)
	,registeredcapacity NUMERIC(6,0)
	,agccapability VARCHAR(1)
	,dispatchtype VARCHAR(20)
	,maxcapacity NUMERIC(6,0)
	,starttype VARCHAR(20)
	,normallyonflag VARCHAR(1)
	,lastchanged DATE
	,upload DATE

	,PRIMARY KEY (effectivedate, duid, versionno)
);

--CREATE INDEX IDX_LASTCHANGED
--ON nem.dudetail (LASTCHANGED);


CREATE TABLE IF NOT EXISTS nem.dudetailsummary(
	 duid VARCHAR(10)
	,start_date DATE
	,end_date DATE
	,dispatchtype VARCHAR(20)
	,connectionpointid VARCHAR(10)
	,regionid VARCHAR(10)
	,stationid VARCHAR(10)
	,participantid VARCHAR(10)
	,lastchanged DATE
	,transmissionlossfactor NUMERIC(15,5)
	,starttype VARCHAR(20)
	,distributionlossfactor NUMERIC(15,5)
	,schedule_type VARCHAR(20)
	,max_ramp_rate_up NUMERIC(6,0)
	,max_ramp_rate_down NUMERIC(6,0)
	,upload DATE

	,PRIMARY KEY (duid, start_date)
);

--CREATE INDEX IDX_LASTCHANGED
--ON nem.dudetailsummary (LASTCHANGED);


CREATE TABLE IF NOT EXISTS nem.registeredgenandload(
	 participant VARCHAR(256)
	,stationname VARCHAR(256)
	,region VARCHAR(10)
	,dispatchtype VARCHAR(20)
	,category VARCHAR(20)
	,classification VARCHAR(20)
	,fuelsourceprimary VARCHAR(256)
	,fuelsourcedescriptor VARCHAR(256)
	,technologytypeprimary VARCHAR(256)
	,technologytypedescriptor VARCHAR(256)
	,aggregation VARCHAR(1)
	,duid VARCHAR(10)
	,geometry_point geometry 
	,upload DATE

	,PRIMARY KEY (duid)
);
