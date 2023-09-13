# Home_Sales

Module 22 big data challenge

This challenges uses SparkSQL to determine key metrics about home sales data.  The home sales data is from an AWS s3 bucket specifically designed for classroom teaching, at https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv.  Although this particular dataset is not that large, the challenge explores tools and techniques for handling big data, and for running spark sql directly from a jupyter notebook.   

After reading the data into a pandas Dataframe, the code properly formats the data types, which all are read as strings, so that arithmetic and date functions can be performed.  The code then creates a temporary view using Spark and runs a number of spark sql queries on the dataset, examining home prices by year built and year sold depending on various home characteristics such as numbers of bedrooms and bathrooms, and using a number of sorting and grouping metrics.  

The code then uses Spark to cache the temporary table.  Some of the same queries are run to compare the query times for the cached and non-cached data.

Finally, the data is written to parquet partitions, in a subfolder called "parquet_sales."  Those parititioins were uploaded to GitHub as part of the package delivery.  The parquet partitions are then read into a new Pandas dataframe, a new temporary view is created, and one of the queries is run against the parquet data, so that the timing can be compared to that of the cached data.  Based on class examples, the query on the parquet data was run twice, to eliminate load times, similar to the cached data.  For the first query run, the times were actually longer than on the cached data, but the second time the same query was run on the paruet data, it was notably shorter, even though the times are all very short with this particular, small test dataset. 

The cached table was then uncached and a test was run to make sure that it had been cleared from cache.  