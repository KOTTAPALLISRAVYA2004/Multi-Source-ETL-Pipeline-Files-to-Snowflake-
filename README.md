The pipeline performs the following steps:

1. Extracts data from multiple sources, including CSV and Excel files.
2. Cleans and standardizes the data (Raw Layer) to ensure consistency and accuracy.
3.Applies business rules and filters (Final Layer) to create a meaningful dataset ready for analysis.
4.Loads the processed data into Snowflake using fast bulk loading with write_pandas.
