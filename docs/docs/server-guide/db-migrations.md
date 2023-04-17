# DB Migrations

## Migrate to Specific Version
Upgrade the DB to the specified migration version.
```shell
$ optimus migration to --version [version]
```
_Note: To migrate to the latest one, running the Optimus’ serve command should be enough. It will migrate to the latest 
automatically._

## Rollback
Revert the current active migration to several previous migration versions.
```shell
$ optimus migration rollback --count [n]
```
[n] is the number of migrations to rollback.

## Export & Upload
In some cases, due to differences in how Optimus is storing the job and resource specifications in DB in 1 version to 
another version, you might want to export the jobs in the server to your local YAML files and redeploy it.

The export is possible using the following command:
```shell
$ optimus job export –-dir job_result
```

This means, you will export all of the jobs from all of the projects in your Optimus server to the job_result directory. It is also possible to run this export command against a single project/namespace/job.

For exporting resources, use the following command:
```shell
$ optimus resource export -dir resource_result
```

Similar to the job command, it is also possible to export resources only for a single project/namespace/resource.
