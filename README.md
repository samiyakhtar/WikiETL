### ETL pipeline for Wikipedia pageview data using Python

#### Project Overview
This project demonstrates an ETL (Extract, Transform, Load) pipeline for Wikipedia pageviews data. The pipeline extracts data from the Wikipedia Pageviews API, transforms it into a structured format, and loads it into a database for analysis. This approach enables users to track and analyze the popularity of different Wikipedia pages over time.

#### Data Source
The data is sourced from the Wikipedia Pageviews API, which provides access to desktop and mobile traffic data for all Wikipedia articles. The API returns data in JSON format with the following key information:
- Article names
- View counts
- Ranking of articles by popularity
- Timestamp information

#### Key Features
- **Data Extraction** from Wikipedia's Pageviews API
- **Data Transformation** with cleaning, validation, and anomaly detection
- **Data Loading** into a SQLite database with proper schema
- **Data Analysis** with visualizations and trend detection
- **Airflow-inspired** workflow organization

#### Technical Skills Demonstrated
- **SQL Knowledge**: Database schema design, complex queries, data manipulation
- **ETL Pipeline Development**: End-to-end data processing workflow
- **API Integration**: Working with RESTful APIs to extract real-world data
- **Data Validation**: Implementing quality checks and anomaly detection
- **Data Visualization**: Creating insightful visual representations of processed data
- **Python Programming**: Pandas, SQLite, JSON processing, and error handling
- **Documentation**: Clear explanation of processes and technical decisions

#### Pipeline Architecture
The ETL pipeline consists of three main components:
1. **Extract**: Pull daily top pageviews data from the Wikipedia Pageviews API
2. **Transform**: Clean and structure the raw JSON data into a normalized format
3. **Load**: Store the processed data in a SQLite database
4. **Analyze**: Create visualizations and extract insights from the processed data

#### Key Visualizations
The pipeline generates several visualizations to provide insights from the processed data:
- Top Wikipedia articles by pageview count
- Trends in article popularity over time
- Anomaly detection in pageview patterns

#### Future Enhancements
- Add more advanced anomaly detection algorithms
- Implement additional data sources for richer analysis
- Create a dashboard for real-time monitoring of Wikipedia trends
- Extend the pipeline to include machine learning predictions
