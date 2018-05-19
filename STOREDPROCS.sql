-- Add a new datasource
DROP PROCEDURE IF EXISTS AddNewDataSource;

DELIMITER $$
CREATE PROCEDURE AddNewDataSource (
    _DataSourceName VARCHAR(50),
    _DataSourceURL VARCHAR(100))
NOT DETERMINISTIC MODIFIES SQL DATA
BEGIN
    IF NOT EXISTS (
            SELECT
                DataSourceName
            FROM
                DataSource
            WHERE
                DataSourceName=_DataSourceName)
    THEN
	    BEGIN
	        INSERT INTO DataSource (
	            DataSourceName,
	            DataSourceURL)
	        VALUES (
	            _DataSourceName,
	            _DataSourceURL);
		END;
    END IF;
END $$
DELIMITER ;

-- Insert a new row of price data. This makes the insertion easier to implement.
DROP PROCEDURE IF EXISTS InsertSecurityPrices;

DELIMITER $$
CREATE PROCEDURE InsertSecurityPrices (
    _Symbol VARCHAR(50),
    _SampleTime DATETIME,
    _OpenPrice FLOAT,
    _HighPrice FLOAT,
    _LowPrice FLOAT,
    _ClosePrice FLOAT,
    _AdjustedClosePrice FLOAT,
    _Volume FLOAT,
    _DividendAmount FLOAT,
    _SplitCoefficient FLOAT,
    _DataSourceID INT)
NOT DETERMINISTIC MODIFIES SQL DATA
BEGIN
    -- Make sure the sample time doesn't already exist before inserting!
    IF NOT EXISTS (
            SELECT
                SampleTime
            FROM
                DataSourcePriceObservation
            WHERE
                SecurityMetaDataID=GetMetaDataIDForSymbol(_Symbol)
                AND
                SampleTime=_SampleTime)
    THEN
		BEGIN
	        INSERT INTO DataSourcePriceObservation (
	            SampleTime,
	            OpenPrice,
	            HighPrice,
	            LowPrice,
	            ClosePrice,
	            AdjustedClosePrice,
	            Volume,
	            DividendAmount,
	            SplitCoefficient,
	            DataSourceID,
	            SecurityMetaDataID)
	        VALUES (
	            _SampleTime,
	            _OpenPrice,
	            _HighPrice,
	            _LowPrice,
	            _ClosePrice,
	            _AdjustedClosePrice,
	            _Volume,
	            _DividendAmount,
	            _SplitCoefficient,
	            _DataSourceID,
	            GetMetaDataIDForSymbol(_Symbol));
	    END;
	END IF;
END $$
DELIMITER ;

-- Add a new symbol to the database, assuming the data soure exists and the symbol does not.
DROP PROCEDURE IF EXISTS AddNewSymbol;

DELIMITER $$
CREATE PROCEDURE AddNewSymbol (
    _Symbol VARCHAR(50),
    _SecurityType VARCHAR(50),
    _SecurityTimeZone VARCHAR(50),
    _SecurityContractSize FLOAT,
    _SecurityDenominationCurrency VARCHAR(3),
    _DataSourceID INT)
NOT DETERMINISTIC MODIFIES SQL DATA
BEGIN
    -- Make sure the DataSoureID is already defined.
    IF NOT EXISTS(SELECT DataSourceID FROM DataSource WHERE DataSourceID=_DataSourceID)
	THEN
		SELECT 'DataSource doesn''t exist' AS '';
	ELSEIF
		EXISTS(SELECT 1 FROM SecurityMetaData WHERE SecuritySymbol=_Symbol)
	    -- Make sure the symbol isn't already in metadata.
		THEN
			SELECT 'Symbol already exists' AS '';
	ELSE
	    BEGIN
	        INSERT INTO SecurityMetaData (
	            SecuritySymbol,
	            SecurityType,
	            SecurityTimeZone,
	            SecurityContractSize,
	            SecurityDenominationCurrency,
	            DataSourceID)
	        VALUES (
	            _Symbol,
	            _SecurityType,
	            _SecurityTimeZone,
	            _SecurityContractSize,
	            _SecurityDenominationCurrency,
	            _DataSourceID);
	    END;
	END IF;
END $$
DELIMITER ;

-- Delete a symbol (and its associated data) from the database
DROP PROCEDURE IF EXISTS DeleteSymbol;

DELIMITER $$
CREATE PROCEDURE DeleteSymbol (
    _Symbol VARCHAR(50))
-- NOT DETERMINISTIC MODIFIES SQL DATA
BEGIN
	DECLARE SecurityID VARCHAR(50);
	SELECT
		SecurityMetaDataID INTO SecurityID
	FROM
		SecurityMetaData
	WHERE
		SecuritySymbol=_Symbol;

    DELETE FROM
        DataSourcePriceObservation
    WHERE
        SecurityMetaDataID=SecurityID;

    DELETE FROM
        SecurityMetaData
    WHERE
        SecuritySymbol=_Symbol;
END $$
DELIMITER ;


-- Get the highest and lowest close for all securities between @start_date and @end_date
DROP PROCEDURE IF EXISTS GetTrailingHighestAndLowestClose;

DELIMITER $$
CREATE PROCEDURE GetTrailingHighestAndLowestClose(
    _start_date DATETIME,
    _end_date DATETIME)
NOT DETERMINISTIC READS SQL DATA
BEGIN
    SELECT
        SecuritySymbol,
        GetAdjustedCloseOnDate(SecuritySymbol, _end_date) AS LastPrice,
        GetMinAdjustedCloseOverDateRange(SecuritySymbol, _start_date, _end_date) AS MinPrice,
        GetMaxAdjustedCloseOverDateRange(SecuritySymbol, _start_date, _end_date) AS MaxPrice
    FROM
        GetSymbolsInDatabase AS tmp;
END $$
DELIMITER ;

-- Show a summary of price range and percent range for all securities between @start_date and @end_date.
DROP PROCEDURE IF EXISTS GetPriceRangeOverDateRange;

DELIMITER $$
CREATE PROCEDURE GetPriceRangeOverDateRange(
    _start_date DATETIME,
    _end_date DATETIME)
DETERMINISTIC READS SQL DATA
BEGIN
    SELECT
        GetSymbolForMetaDataID(SecurityMetaDataID) AS Symbol,
        MAX(AdjustedClosePrice) - MIN(AdjustedClosePrice) AS PriceRange,
        (MAX(AdjustedClosePrice) - MIN(AdjustedClosePrice)) / AVG(AdjustedClosePrice) AS PctRange
    FROM
        DataSourcePriceObservation
    WHERE
        SampleTime >= _start_date
    AND
        SampleTime <= _end_date
    GROUP BY
        Symbol;
END $$
DELIMITER ;

-- Helper functionto return all available time series data for @symbol.
-- This can be used by e.g. Python to easily load data required for additional analysis.
DROP PROCEDURE IF EXISTS GetSecurityData;

DELIMITER $$
CREATE PROCEDURE GetSecurityData(
    symbol VARCHAR(50))
NOT DETERMINISTIC READS SQL DATA
BEGIN
    SELECT
        SampleTime,
        OpenPrice,
        HighPrice,
        LowPrice,
        ClosePrice,
        AdjustedClosePrice,
        Volume,
        OpenInterest,
        DividendAmount,
        SplitCoefficient
    FROM
        DataSourcePriceObservation
    WHERE
        SecurityMetaDataID=GetMetaDataIDForSymbol(symbol);
END $$
DELIMITER  ;

-- Helper function to get the (adjusted) closing price @symbol between @start_date and @end_date.
DROP PROCEDURE IF EXISTS GetAdjustedCloseOverDateRange;

DELIMITER $$
CREATE PROCEDURE GetAdjustedCloseOverDateRange(
    symbol VARCHAR(50),
    start_date DATETIME,
    end_date DATETIME)
DETERMINISTIC READS SQL DATA
BEGIN
    SELECT
        SampleTime,
        AdjustedClosePrice
    FROM
        DataSourcePriceObservation
    WHERE
        SecurityMetaDataID=GetSymbolForMetaDataID(symbol)
    AND
        SampleTime >= start_date
    AND
        SampleTime <= end_date;
END $$
DELIMITER ;

-- Helper function to return the most recent @length adjusted closing price observations for @symbol.
DROP PROCEDURE IF EXISTS GetLastNAdjustedCloseObservations;

DELIMITER $$
CREATE PROCEDURE GetLastNAdjustedCloseObservations(
    _symbol VARCHAR(30),
    _length INT)
NOT DETERMINISTIC READS SQL DATA
BEGIN
    SELECT
        SampleTime,
        AdjustedClosePrice
    FROM
        DataSourcePriceObservation
    WHERE
        SecurityMetaDataID=GetSymbolForMetaDataID(symbol)
    ORDER BY
        SampleTime DESC
    LIMIT _length;
END $$
DELIMITER ;

-- Show symbols with most recent close above the @length period average.
DROP PROCEDURE IF EXISTS GetSecuritiesAboveAvg;

DELIMITER $$
CREATE PROCEDURE GetSecuritiesAboveAvg(
    _length INT)
NOT DETERMINISTIC READS SQL DATA
BEGIN
    SELECT
        SecuritySymbol,
        AdjustedClosePrice AS Price,
        GetAvgPrice(SecuritySymbol, _length) AS Avg
    FROM
        GetLatestClosePrices as tmp
    WHERE
        AdjustedClosePrice > GetAvgPrice(SecuritySymbol, _length);
END $$
DELIMITER ;

-- Show symbols with most recent close below the @length period average.
DROP PROCEDURE IF EXISTS GetSecuritiesBelowAvg;

DELIMITER $$
CREATE PROCEDURE GetSecuritiesBelowAvg(
    _length INT)
NOT DETERMINISTIC READS SQL DATA
BEGIN
    SELECT
        SecuritySymbol,
        AdjustedClosePrice AS Price,
        GetAvgPrice(SecuritySymbol, _length) AS Avg
    FROM
        GetLatestClosePrices AS tmp
    WHERE
        AdjustedClosePrice < GetAvgPrice(SecuritySymbol, _length);
END $$
DELIMITER ;
