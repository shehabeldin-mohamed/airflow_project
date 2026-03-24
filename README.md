# airflow_project
This project showcases a local airflow setup with mongo databases. The pipeline starts by loading a csv file, cleans it and then load the dataset into mongodb database.

## Prerequisites
.env file containing the following: 
* AIRFLOW_UID=50000 
* _PIP_ADDITIONAL_REQUIREMENTS=pandas pymongo apache-airflow-providers-mongo

## Dag 1
![Airflow DAG 1 showing data extraction](assets/dag1.png)

## Dag 2
![Airflow DAG 2 showing data loading](assets/dag2.png)

## Execution of both dags after each other
![Airflow Execution of both dags after each other](assets/both_dags.png)

## Cleaned Dataset In MongoDb Compass
![Mongodb Cleaned Dataset](assets/mongodb_compass_intial.png)

## Query 1
![First Query](assets/top5_frequently_occuring_comments.png)

## Query 2
![SecondQuery](assets/content_less_than_5_characters.png)

## Query 3
![Third Query](assets/avg_rating_each_day.png)
