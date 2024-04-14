Data Overview:
The data used in aggregate by the SQL database is oceanic sensor data from NOAA.
Detailed value descriptions of NOAA datatypes can be found here:
https://github.com/partytax/ncei-api-guide?tab=readme-ov-file#detailed-value-descriptions

The data used in sensors and expeditions are dummy data generated for 
demonstration only.

Setup Instructions:
To setup the SQL server, simply log into mysql with --local-infile=1 and run 
"source setup.sql" in an empty database.

The python application only requires to be run and terminal functionalities will
be made available and explained.

Written Files:
The export functionality of the terminal will convert the currently selected
mysql table into a CSV stored within the csv_output folder




