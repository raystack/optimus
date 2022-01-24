- Feature Name: Revisit Automated Dependency Resolution Logic
- Status: Draft
- Start Date: 2022-01-24
- Authors: 

# Summary

Optimus is a data warehouse management system, data is at the core of Optimus. Automated dependency resolution is the core problem which Optimus wants to address. The current windowing is confusing, so lets take a revisit to understand how we can solve this problem if we are tackling it fresh.

# Technical Design

### Background : 

Input Data Flows through the system & it is expected to arrive at some delay or user gives enough buffer for all late data to arrive. Post that, the user expects to schedule the job to process the data after the max delay.

Keeping this basic idea in mind, what logic can be used to enable automated dependency resolution is the key question for us? And what all questions need to be answered for the same?

Question 1 : What is the time range of data a job consumes from the primary sources?

Question 2 : What is the time range of data a job writes?

If these two questions be answered for every scheduled job then dependent jobs be computed accordingly.

### Approach :

Let's answer the **Question 1**, this is clearly a user input, there is no computation here. How intuitively a user input can be taken is the key here.

```yaml
data_window : 
 max_delay : 1d/2h/1d2h
 amount : 1d/1w/2d/1m 
```

Let's answer **Question 2**, I believe this is mainly linked with the schedule of the job, if the job is scheduled daily then the expectation is the job makes data available for a whole day, if hourly then for a whole hour, irrespective of the input_window. What exactly is the time range can be computed by `max_delay` and `schedule_frequency` . 

If the job has a max_delay of 2h & the job is scheduled at 2 AM UTC then the job is making the data available for the entire previous day, irrespective of the window(1d,1m,1w).

WIth this core idea there are few scenarios which should not be allowed or which cannot be used for automated depedency resolution to work in those cases the jobs just depend on the previous jobs for eg., dynamic schedules. If a job is scheduled only 1st,2nd & 3rd hour of the day.

The next part of the solution is how to do the actual dependency resolution.

1. Compute the data_window based on user input & the schedule time.
2. Identify all the upstreams.
3. Starting from the current schedule_time get each upstream's schedule in the past & future and compute the output data range till it finds runs which falls outside the window in each direction.

### Other Thoughts:

Inorder to keep things simple for most of the users, if a user doesn't define any window then the jobs depend on the immediate upstream by schedule_time for that job & as well as the jobs that are depending the above job.

`dstart` & `dend` macros will still be offered to users which they can use for data filtering in their queries.

### How do we transition to this new approach?

1. Support Old & New approaches both, migrate all jobs to the new strategy & later cleanup the code.