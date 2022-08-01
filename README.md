
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<p align="center">

  <h2 align="center">AirFlow-ETL</h2>

  <p align="center">
    ETL pipline build on AirFlow  
    <br />
    ·
    <a href="https://github.com/angelxd84130/Airflow-ETL/issues">Report Bug</a>
    ·
    <a href="https://github.com/angelxd84130/Airflow-ETL/issues">Request Feature</a>
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>  
        <li><a href="#structure">Structure</a></li>  
        <li><a href="#etl">ETL</a></li>  
        <li><a href="#evaluation">Evaluation</a></li>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project


The goal is building a ETL pipline on AirFlow to auto get data from Google BigQuery and storage in the local database MySQL after process.    


Here's why:
* AirFlow scheduling system can start ETL pipeline at a fixed time  
* Re-run program in fixed times when meeting operational errors    
* Quickly check running results and notice errors on the AirFlow Dags panel  
* Simply research running records through the log file system   
* Obtain and store the required data after the ETL process  
  
### Structure  
![AirFlow-ETL][product-screenshot0]   
The whole architecture consists of two independent systems, one is AirFlow-ETL and the other is a Real-time Dashboard.  
This system runs the ETL program regularly through the AirFlow system, uses the JSON key to obtain event tracking data from Google BigQuery, converts the data, 
and stores it in the local database MySQL for access by another dashboard system.  

### ETL  
Every 10 minutes, the transaction data of the day is requested from BigQuery, 
classifying the transaction results and error codes, and then updating data in MySQL.   
 

#### Extract  
Using JSON key accesses tables on BigQuery, and load daily data through SQL.   
Since the database time zone on BigQuery is different from the local one, the UTC+8 time zone problem must be dealt with before using SQL to retrieve data.
```
# bigquery_eventtracking_regular_report.py
startTime = (datetime.now() + timedelta(days=-n-1)).strftime('%Y-%m-%d') + 'T16:00:00'
endTime = (datetime.now() + timedelta(days=-n)).strftime('%Y-%m-%d') + 'T16:00:00'
```

#### Transform  
Store the obtained data as a DataFrame, and then add columns after procession.    
```
# parse.py
df['event_date'] = eventTime
df['error_rate'] = df['sum_of_error'] / sum(df['sum_of_error'])
```

#### Load    
Insert/ Update transformed data on MySQL through SQL.  
- If the row data already in the db -> update the value.  
- Else -> Insert the row data.  
```
# storage.py

logging.info('searching exist in db..' + error_code)
sql = f"""
		SELECT * From {table} 
		WHERE event_date="{eventTime}" and error_code="{error_code}" and sport_code="{sport_code}"; 
	"""
cur.execute(sql)

if(cur.fetchone()):
    logging.info('updating data..')
    sql = f""" 
		UPDATE {table} SET sum_of_error = {sum_of_error} 
		WHERE event_date = "{eventTime}" and error_code = "{error_code}" and sport_code="{sport_code}";
	""" 
else:
    logging.info('inserting data..')
    sql = f"""
		INSERT INTO {table} (event_date, sport_code, error_code, sum_of_error)
		VALUES ("{eventTime}", "{sport_code}", "{error_code}", {sum_of_error}) ;
        """ 
cur.execute(sql)
conn.commit()
```

### Evaluation  
1. Check whether the dag file runs every 10 mins  
AirFlow Dag Panel  
![AirFlow-Dag][product-screenshot1]  
  
  
2. Evaluate the data is written in database correctly  
Table : sport_transaction_error  
![sport_transaction_error][product-screenshot2]   
Table : sport_transaction_result  
![sport_transaction_result][product-screenshot3]    
  
  
### Built With

* [AirFlow](https://airflow.apache.org/)
* [Pandas](https://pandas.pydata.org/)
* [MySQL](https://www.mysql.com/)
* [SQL](https://www.w3schools.com/sql/)



<!-- GETTING STARTED -->
## Getting Started

Download the whole project except this README file and the pic folder, and move the project under the path: /airflow/dags/  

### Prerequisites


1. Install AirFlow and set up the Panel  
2. Replace BigQuery JSON key and table name to your own.
   ```sh
   # _query.py
   credential_path = "/home/albert/airflow/dags/bigquery_eventtracking_regular_report_module/sg-prod-readonly-303206-cb8365379fd6.json"  
   ```
3. Install a local database MySQL & Create tables      
4. Create a virtual enviroment for airflow test tasks   
   ```  
   source airflow_venv/bin/activate
   ```  
5. Clone the project into the enviroment and check if it's runnable  
   - Check whether the dag file is readable for the airflow dag list  
   ```  
   airflow dags list
   ```  
   - Check whether the dag tasks is detectable for the task list  
   ```  
   airflow tasks list <dag_id>
   ```  
   - Check whether the tasks in the dag are runnable  
   ```  
   airflow tasks test <dag_id> <task_id> <start_time>
   ```    
   (time formate ex. yyyy-mm-dd)  
6. Close the airflow virtul evniroment  
   ```  
   deactivate  
   ```  
7. Clone the project again into the real enviroment under /airflow/dags   
8. Check whether the dag show up on the AirFlow Dag Panel  



<!-- USAGE EXAMPLES -->
## Usage  
The structure is workable for every ETL process,  
and the AirFlow system helps to centrally manage all tasks and instantly detect errors in operation.  



<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/othneildrew/Best-README-Template/issues) for a list of proposed features (and known issues).


<!-- CONTACT -->
## Contact

Yu-Chieh Wang - [LinkedIn](https://www.linkedin.com/in/yu-chieh-wang/)  
email: angelxd84130@gmail.com


<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [Multi-Class Text Classification with Scikit-Learn](https://towardsdatascience.com/multi-class-text-classification-with-scikit-learn-12f1e60e0a9f)
* [Text Classification Using Naive Bayes: Theory & A Working Example](https://towardsdatascience.com/text-classification-using-naive-bayes-theory-a-working-example-2ef4b7eb7d5a)





<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/angelxd84130/Airflow-ETL.svg?style=for-the-badge
[contributors-url]: https://github.com/angelxd84130/Airflow-ETL/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/angelxd84130/Airflow-ETL.svg?style=for-the-badge
[forks-url]: https://github.com/angelxd84130/Airflow-ETL/network/members
[stars-shield]: https://img.shields.io/github/stars/angelxd84130/Airflow-ETL.svg?style=for-the-badge
[stars-url]: https://github.com/angelxd84130/Airflow-ETL/stargazers
[issues-shield]: https://img.shields.io/github/issues/angelxd84130/Airflow-ETL.svg?style=for-the-badge
[issues-url]: https://github.com/angelxd84130/Airflow-ETL/issues
[license-shield]: https://img.shields.io/github/license/angelxd84130/Airflow-ETL.svg?style=for-the-badge
[license-url]: https://github.com/angelxd84130/Airflow-ETL/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/yu-chieh-wang/
[product-screenshot0]: /bigquery_eventtracking_regular_report_module/pic/AirFlow-ETL.png
[product-screenshot1]: /bigquery_eventtracking_regular_report_module/pic/AirFlow-Dag.png
[product-screenshot2]: /bigquery_eventtracking_regular_report_module/pic/sport_transaction_error.png
[product-screenshot3]: /bigquery_eventtracking_regular_report_module/pic/sport_transaction_result.png
