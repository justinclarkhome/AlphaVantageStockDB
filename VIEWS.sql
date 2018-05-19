-- Show the latest closing prices for all securities in the database.
DROP VIEW IF EXISTS GetLatestClosePrices;

CREATE VIEW GetLatestClosePrices AS
SELECT
	dspo.SampleTime,
	dspo.AdjustedClosePrice,
	smd.SecuritySymbol
FROM
	DataSourcePriceObservation AS dspo
JOIN
	SecurityMetaData AS smd ON dspo.SecurityMetaDataID=smd.SecurityMetaDataID
WHERE
	dspo.Sampletime=(
		SELECT
            MAX(SampleTime)
        FROM
            DataSourcePriceObservation);

-- Get a list of all the symbols in the database
DROP VIEW IF EXISTS GetSymbolsInDatabase;

CREATE VIEW GetSymbolsInDatabase AS
SELECT
	smd.SecuritySymbol, ds.DataSourceName
FROM
	SecurityMetaData AS smd
JOIN
	DataSource AS ds on smd.DataSourceID=ds.DataSourceID;

-- query to get the last day of each month (According to data in the database)
DROP VIEW IF EXISTS GetMonthEndDates;

CREATE VIEW GetMonthEndDates AS
SELECT DISTINCT
    SampleTime
FROM
    DataSourcePriceObservation
WHERE SampleTime IN (
    SELECT
        MAX(SampleTime)
    FROM
        DataSourcePriceObservation
    GROUP BY
        MONTH(SampleTime),
        YEAR(SampleTime)
    );

