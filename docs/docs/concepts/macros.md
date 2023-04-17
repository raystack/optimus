# Macros
Macros are special variables that will be replaced by actual values when before execution. Custom macros are not 
supported yet.

Below listed the in built macros supported in optimus.

| Macros               | Description                                                                     |
| -------------------- |---------------------------------------------------------------------------------|
| {{.DSTART}}          | start date/datetime of the window as 2021-02-10T10:00:00+00:00 that is, RFC3339 |
| {{.DEND}}            | end date/datetime of the window, as RFC3339                                     |
| {{.JOB_DESTINATION}} | full qualified table name used in DML statement                                 |
| {{.EXECUTION_TIME}}  | timestamp when the specific job run starts                                      |

Take a detailed look at the windows concept and example [here](intervals-and-windows.md).
