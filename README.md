# SUDS_Residential_Fire
Model for Residential Fire Risk prediction

How to run:
Put these files in the same directory as the script files:
parcels.csv, pittdata.csv, acs_income.csv, acs_occupancy.csv, acs_year_built.csv, acs_year_moved.csv, pli.csv, Fire_Incidents_Pre14.csv, Fire_Incidents_Historical.csv

Run ResidentialCleaning.py. This should create 3 new csv files:
pittdata_blocks.csv, plidata_blocks.csv, fire_incident_blocks.csv

Run ResidentialModel.py
