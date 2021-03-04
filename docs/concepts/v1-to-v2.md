# What's new in v2

- Binary is called `opctl` now instead of optimus.
- There are no dags, tasks anymore. Specifications are now called job and there is
  only a single file for it.
- There is no properties.cfg file, all these configs goes in same job.yaml file.
- Jobs have a single `asset` folder where the query should go for BQ transformation.
- Task window configuration which was specified in `properties.cfg` earlier also goes in 
  `job.yaml`.
- Macros are not formatted differently and follows golang template conventions.
  instead of simply calling them via name, they needs to be enclosed in curly braces.
  For example:
```sql
Select * from sometable where event_time < "{{.DSTART}}"
```
- `query.sql` and all other asset files now supports functions evaluations defined 
  at golang [docs](https://golang.org/pkg/text/template/) and [sprig](http://masterminds.github.io/sprig/) 
  library.

