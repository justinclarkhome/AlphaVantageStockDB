-- Get SecurityMetaDataID associated with @symbol.
DROP FUNCTION IF EXISTS GetMetaDataIDForSymbol;

DELIMITER $$
CREATE FUNCTION GetMetaDataIDForSymbol(
    symbol VARCHAR(50))
RETURNS INT
DETERMINISTIC READS SQL DATA
BEGIN
    DECLARE ANSWER INT;
    SELECT
        SecurityMetaDataID INTO ANSWER
    FROM
        SecurityMetaData
    WHERE
        SecuritySymbol = symbol;
    RETURN ANSWER;
END $$
DELIMITER ;

-- Get the symbol associated with @SecurityMetaDataID
DROP FUNCTION IF EXISTS GetSymbolForMetaDataID;

DELIMITER $$
CREATE FUNCTION GetSymbolForMetaDataID(SecurityMetaDataID INT)
RETURNS VARCHAR(50)
DETERMINISTIC READS SQL DATA
BEGIN
	DECLARE answer VARCHAR(50);
	SELECT
		SecuritySymbol INTO answer
	FROM
		SecurityMetaData
	WHERE
		SecurityMetaData.SecurityMetaDataID=SecurityMetaDataID;
	RETURN answer;
END $$
DELIMITER ;

-- Helper function to return the most recent SampleTime recorded for @symbol.
-- This can be used to identify how much new data needs to be appended by the update scripts.
DROP FUNCTION IF EXISTS GetLastSampleTimeForSecurity;

DELIMITER $$
CREATE FUNCTION GetLastSampleTimeForSecurity(
    symbol VARCHAR(50))
RETURNS DATETIME
NOT DETERMINISTIC READS SQL DATA
BEGIN
    DECLARE ANSWER DATETIME;
    SELECT
        MAX(SampleTime) INTO ANSWER
    FROM
        DataSourcePriceObservation
	WHERE
		SecurityMetaDataID=GetMetaDataIDForSymbol(symbol);
    RETURN ANSWER;
END $$
DELIMITER ;

-- Return the AdjustedClosePrice on a specific end_date.
DROP FUNCTION IF EXISTS GetAdjustedCloseOnDate;

DELIMITER $$
CREATE FUNCTION GetAdjustedCloseOnDate(
    symbol VARCHAR(30),
    end_date DATETIME)
RETURNS FLOAT
NOT DETERMINISTIC READS SQL DATA
BEGIN
    DECLARE answer FLOAT;
    SELECT
        AdjustedClosePrice INTO answer
    FROM
        GetLatestClosePrices
    WHERE
        SecuritySymbol=symbol;
    RETURN answer;
END $$
DELIMITER ;

--
DROP FUNCTION IF EXISTS GetMaxAdjustedCloseOverDateRange;

DELIMITER $$
CREATE FUNCTION GetMaxAdjustedCloseOverDateRange(
    symbol VARCHAR(30),
    start_date DATETIME,
    end_date DATETIME)
RETURNS FLOAT
NOT DETERMINISTIC READS SQL DATA
BEGIN
    DECLARE answer FLOAT;
    SELECT
        MAX(AdjustedClosePrice) INTO answer
    FROM
        DataSourcePriceObservation
    WHERE
        SecurityMetaDataID=GetMetaDataIDForSymbol(symbol)
    AND
        SampleTime >= start_date
    AND
        SampleTime <= end_date;
    RETURN answer;
END $$
DELIMITER ;

--
DROP FUNCTION IF EXISTS GetMinAdjustedCloseOverDateRange;

DELIMITER $$
CREATE FUNCTION GetMinAdjustedCloseOverDateRange(
    symbol VARCHAR(30),
    start_date DATETIME,
    end_date DATETIME)
RETURNS FLOAT
NOT DETERMINISTIC READS SQL DATA
BEGIN
    DECLARE answer FLOAT;
    SELECT
        MIN(AdjustedClosePrice) INTO answer
    FROM
        DataSourcePriceObservation
    WHERE
        SecurityMetaDataID=GetMetaDataIDForSymbol(symbol)
    AND
        SampleTime >= start_date
    AND
        SampleTime <= end_date;
    RETURN answer;
END $$
DELIMITER ;

-- Get the average adjusted closing price for @symbol over the last @length periods.
DROP FUNCTION IF EXISTS GetAvgPrice;

DELIMITER $$
CREATE FUNCTION GetAvgPrice(
    _symbol VARCHAR(30),
    _length INT)
RETURNS FLOAT
NOT DETERMINISTIC READS SQL DATA
BEGIN
    DECLARE answer FLOAT;
    SELECT
        AVG(AdjustedClosePrice) INTO answer
    FROM
        DataSourcePriceObservation
    WHERE
        SecurityMetaDataID=GetMetaDataIDForSymbol(_symbol)
    ORDER BY
        SampleTime DESC
    LIMIT _length;
    RETURN answer;
END $$
DELIMITER ;

-- Show the annual percent change for @symbol during @refyear
DROP FUNCTION IF EXISTS YearlyChange;

DELIMITER $$
CREATE FUNCTION YearlyChange(symbol VARCHAR(50), refyear INT)
RETURNS FLOAT
DETERMINISTIC READS SQL DATA
BEGIN
	DECLARE BegSampleTime DATETIME;
	DECLARE EndSampleTime DATETIME;
	DECLARE BegPrice FLOAT;
	DECLARE EndPrice FLOAT;

	SELECT
        MIN(SampleTime), MAX(SampleTime)
    INTO
        BegSampleTime, EndSampleTime
	FROM
		DataSourcePriceObservation
	WHERE
		YEAR(Sampletime) = refyear
        AND
		SecurityMetaDataID=GetMetaDataIDForSymbol(symbol)
        AND
        AdjustedClosePrice IS NOT NULL;

	SELECT AdjustedClosePrice INTO BegPrice
	FROM
		DataSourcePriceObservation
	WHERE
		SampleTime=BegSampleTime
        AND
		SecurityMetaDataID=GetMetaDataIDForSymbol(symbol);

	SELECT AdjustedClosePrice INTO EndPrice
	FROM
		DataSourcePriceObservation
	WHERE
		SampleTime=EndSampleTime
        AND
		SecurityMetaDataID=GetMetaDataIDForSymbol(symbol);

	RETURN EndPrice / BegPrice - 1.0;
END $$
DELIMITER ;

-- Get DataSourceID associated with DataSourceName
DROP FUNCTION IF EXISTS GetDataSourceIDFromDataSourceName;

DELIMITER $$
CREATE FUNCTION GetDataSourceIDFromDataSourceName (_data_source_name VARCHAR(50))
RETURNS INT
DETERMINISTIC READS SQL DATA
BEGIN
    DECLARE answer INT;
    SELECT
        DataSourceID INTO answer
    FROM
        DataSource
    WHERE
        DataSourceName=_data_source_name;
    RETURN answer;
END $$
DELIMITER ;

-- Get DataSourceName from DataSourceID
