# Uploading Job to Scheduler

Compile and upload all jobs in the project by using this command:
```shell
$ optimus scheduler upload-all
```
_Note: add --config flag if you are not in the same directory with your client configuration (optimus.yaml)._

This command will compile all of the jobs in the project to Airflow DAG files and will store the result to the path 
that has been set as `STORAGE_PATH` in the project configuration. Do note that `STORAGE` secret might be needed if 
the storage requires a credential.

Once you have the DAG files in the storage, you can sync the files to Airflow as you’d like.
