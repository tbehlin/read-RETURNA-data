# Reading the Fixed-Width RETURN-A Data

 a python notebook that describes and demonstrates one method for reading the data from a fixed-width FBI RETURN-A file. This method is reasonably effective, and could be adapted to a different Fixed-Width file, given an adapted Key-File.



The RETURN-A files are not included in this repository, but can be obtained from https://cde.ucr.cjis.gov/LATEST/webapp/#, under Documents & Downloads, then Master File Downloads. Available at that location is the RETURN-A data from 1985 onwards, representing the crime data for every police agency within the United States.





Due to data quality constraints, this file automatically aggregates the crime data to the agency-year level, as there is inconsistent data treatment at the agency-month level.



This process makes use of the pandas .str accessor, and is therefore significantly faster than a more manual interpretation, and is able to read, aggregate, and save the 100-200MB fixed-width RETURN-A files in about a minute for each file.



Also included is the machinery to merge in the State, County, and Place FIPS codes, using a crosswalk file that can be obtained from  https://www.icpsr.umich.edu/web/ICPSR/studies/35158#.

This file is optional, and the notebook can be run without it.
