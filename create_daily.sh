# Code for (re)creating the databse on MySQL/MariaDB Server
dbuser='myname'
dbuserpw='myP@ssword'
dbname='PRICES_DAILY'

echo "Dropping $dbname (if it exists)"
mysql -u "${dbuser}" --password="${dbuserpw}" -e "DROP DATABASE ${dbname}"

echo "Creating new $dbname"
mysql -u "${dbuser}" --password="${dbuserpw}" -e "CREATE DATABASE ${dbname}"

echo "Creating tables"
mysql -u "${dbuser}" --password="${dbuserpw}" "${dbname}" < ./code_mysql/CREATES.sql

echo "Creating Views"
mysql -u "${dbuser}" --password="${dbuserpw}" "${dbname}" <  ./code_mysql/VIEWS.sql

echo "Creating Functions"
mysql -u "${dbuser}" --password="${dbuserpw}" "${dbname}" < ./code_mysql/FUNCTIONS.sql

echo "Creating Stored Procedures"
mysql -u "${dbuser}" --password="${dbuserpw}" "${dbname}" < ./code_mysql/STOREDPROCS.sql
