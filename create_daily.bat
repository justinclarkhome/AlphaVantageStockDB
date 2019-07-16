::Code for (re)creating the database on MySQL/MariaDB Server

@echo off
set /P dbuser="Enter your MySQL username (and press enter):"
set /P dbuserpw="Enter your MySQL password (and press enter):"

set dbname=PRICES_DAILY

echo "Dropping %dbname% (if it exists)"
mysql -u %dbuser% --password=%dbuserpw% -e "DROP DATABASE %dbname%

echo "Creating new %dbname%"
mysql -u %dbuser% --password=%dbuserpw% -e "CREATE DATABASE %dbname%

echo "Creating tables"
mysql -u %dbuser% --password=%dbuserpw% %dbname% < .\code_mysql\CREATES.sql

echo "Creating Views"
mysql -u %dbuser% --password=%dbuserpw% %dbname% <  .\code_mysql\VIEWS.sql

echo "Creating Functions"
mysql -u %dbuser% --password=%dbuserpw% %dbname% < .\code_mysql\FUNCTIONS.sql

echo "Creating Stored Procedures"
mysql -u %dbuser% --password=%dbuserpw% %dbname% < .\code_mysql\STOREDPROCS.sql
