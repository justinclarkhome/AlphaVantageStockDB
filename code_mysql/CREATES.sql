-- First drop any existing tables. Order matters because of foreign key dependencies!
DROP TABLE IF EXISTS DataSourcePriceObservation;
DROP TABLE IF EXISTS SecurityMetadata;
DROP TABLE IF EXISTS DataSource;

-- Now recreate them in the proper order.

-- Table that stores datasource information.
CREATE TABLE DataSource (
    -- required fields
    DataSourceID INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    DataSourceName VARCHAR(50) NOT NULL,
    DataSourceURL VARCHAR(100) NOT NULL
);

-- Table that stores security metadata information.
CREATE TABLE SecurityMetaData (
    -- required fields:
    SecurityMetaDataID INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    SecuritySymbol VARCHAR(50) NOT NULL,
    SecurityType VARCHAR(50) NOT NULL,
    SecurityTimeZone VARCHAR(5) NOT NULL,
    SecurityContractSize FLOAT NOT NULL,
    SecurityDenominationCurrency CHAR(3) NOT NULL,
    -- foreign keys:
    DataSourceID INT REFERENCES DataSource(DataSourceID)
);

-- Table that stores individual observations of data for each security.
CREATE TABLE DataSourcePriceObservation (
    -- required fields:
    DataSourcePriceObservationID INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    SampleTime DATETIME NOT NULL,
    OpenPrice FLOAT,
    HighPrice FLOAT,
    LowPrice FLOAT,
    ClosePrice FLOAT,
    AdjustedClosePrice FLOAT,
    -- optional fields:
    Volume FLOAT,
    DividendAmount FLOAT,
    SplitCoefficient FLOAT,
    OpenInterest FLOAT,
    -- foreign keys:
    DataSourceID INT REFERENCES DataSource(DataSourceID),
    SecurityMetaDataID INT REFERENCES SecurityMetaData(SecurityMetaDataID)
);


-- Index between MetaData ID and Symbol
-- DROP INDEX MetaDataIDAndSymbolIndex ON SecurityMetaData;

CREATE UNIQUE INDEX
	MetaDataIDAndSymbolIndex
ON
    SecurityMetaData (SecurityMetaDataID, SecuritySymbol);

