# WeatherDatapipeline Practice
![Screenshot](https://github.com/abhinandanshrestha/WeatherDatapipeline/assets/43780258/3e7958db-4398-4cc1-ac21-e9cea4c9ea94)

Used WeatherAPI (www.weatherapi.com) trial to extract data from the last 7 days and then saved it as a blob in a blob container called "input" which was created in Azure Storage Service. Refer to main.py. 
Then, used Data Factory to create a pipeline with Copy Activity from blob to Azure SQL Database Service. For creating a pipeline, ensure that you have Linked Service and also Dataset created for both Source (blob) and Sink (SQL Server). 
After running the pipeline, data is successfully loaded into the SQL Databases. From SQL Databases, we can run Power BI directly by downloading a file. 

Screenshots from Power BI:
![Screenshot (7)](https://github.com/abhinandanshrestha/WeatherDatapipeline/assets/43780258/92eb6e47-267a-470d-9d85-65ecaba18e12)
![Screenshot (8)](https://github.com/abhinandanshrestha/WeatherDatapipeline/assets/43780258/6d0ea3e2-93b5-44db-a69b-1ecd1fe0958b)
![Screenshot (9)](https://github.com/abhinandanshrestha/WeatherDatapipeline/assets/43780258/7173e21a-f7da-4cea-97ff-a1f7dce93869)
![Screenshot (10)](https://github.com/abhinandanshrestha/WeatherDatapipeline/assets/43780258/b6984e56-8383-4634-a3a0-ac46d8443f0c)

For a pdf of the visualization, refer to visualization.pdf


