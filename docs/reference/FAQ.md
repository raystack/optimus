# FAQ

- **I want to run a DDL/DML query like DELETE, how can I do that?**
  
  Write SQL query as you would write in BQ UI and select the load method as MERGE. 
  This will execute even if the query does not contain a merge statement.

- **What should not be changed once the specifications are created?**
  
  Optimus uses Airflow for scheduling job execution and it does not support change of start_date & schedule_interval once the dag is created. For that please delete the existing one or recreate it with a different suffix. Also make sure you don’t change the dag name if you don’t want to lose the run history of Airflow.
