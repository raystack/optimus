# FAQ

- **I want to run a DDL/DML query like DELETE, how can I do that?**
  
  Write SQL query as you would write in BQ UI and select the load method as MERGE. 
  This will execute even if the query does not contain a merge statement.

- **What should not be changed once the specifications are created?**
  
  Optimus uses Airflow for scheduling job execution and it does not support change of start_date & schedule_interval 
  once the dag is created. For that please delete the existing one or recreate it with a different suffix. 
  Also make sure you don’t change the dag name if you don’t want to lose the run history of Airflow.

- **Can I have a job with only a transformation task (without a hook) or the other way around?**

  Transformation task is mandatory but hook is optional. You can have a transformation task without a hook but cannot 
  have a hook without a transformation task.

- **I have a job with a modified view source, however, it does not detect as modified when running the job replace-all command. 
  How should I apply the change?**

  It does not detect as modified as the specification and the assets of the job itself is not changed. Do run the job refresh command.

- **My job is failing due to the resource is not sufficient. Can I scale a specific job to have a bigger CPU / memory limit?**
  
  Yes, resource requests and limits are configurable in the job specification’s metadata field.

- **I removed a resource specification, but why does the resource is not deleted in the BigQuery project?**

  Optimus currently does not support resource deletion to avoid any accidental deletion.

- **I removed a job specification, but why does it still appear in Airflow UI?**

  You might want to check the log of the replace-all command you are using. It will show whether your job has been 
  successfully deleted or not. If not, the possible causes are the job is being used by another job as a dependency. 
  You can force delete it through API if needed.

  If the job has been successfully deleted, it might take time for the deletion to reflect which depends on your Airflow sync or DAG load configuration.
