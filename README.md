## Spark Take Home Assignment
### Introduction
Thanks for reviewing my work regarding this interesting problem with Spark. Please find the files in this folder as:

* `assignment.py` the PySpark job to load the dataset, transform, and persist results to csv file.
* `tests.py` Pytest-based unit tests.
* `result.csv` Result as requested in tab separated format
* `README.md` This Markdown README file.

### Solutions

I tried both local and cloud for this interesting problem. 

For local solution, I lunched a three work nodes Spark cluster on my M1 MacBookPro. And submit the `assignment.py` as a Spark job to the local cluster. With my hardware, it took about 30 mins.

For cloud solution, I run the same logic on Azure Databricks in a notebook. With a Photon-accelerated cluster, it took about 5 mins. 

Both solution yields the same result. But the local cluster gives me more flexibility to tune the environment and I am actually learning a lot from this process.

### Tools/Platform

For local approach, I borrowed an existing project from Github https://github.com/mrn-aglic/pyspark-playground to setup local cluster. I did actually had to tweak the Dockerfile and Docker Compose yaml a bit to make sure it can still build (lack of maintenance for a while). 

Then I was able to launch the cluster as
```
$ make run-scaled
```

And then once the cluster is provisioned, I can submit the job as:

```
$ make submit app=assignment.py
```

In order to run the unit tests locally, you'll need to install `pyspark==3.3.1`, `pytest`, and JDK.

For cloud approach, it's much straightforward. Open a notebook and bring in the logics in that `assignment.py`, please do make sure that the read/persist paths are modified as it is to point to mounted storage point in a data lake.

### Discussion
This is a fun project and I wish I have more time to work on it. If I have sufficient time, I plan to build more unit tests, and tune more on the Spark config to find a good balance to run my job on my local cluster. For the cloud approach it's more about using which beefy compute node. Sometimes in a real work scenario, we might need to balance on the engineering hour and compute cost. If a slight more compute cost save a tons of engineering hours, I would say go for it. But for a project like this, or sometimes potentially large dataset that vertical scaling is an issue, tuning will be very important.

If productionize this work, I might think that this is a part of the pipeline, I'd like to have orchestration tools involved, like Airflow or Prefect. I might also want to have observability tools setup to monitor the source availability and data integrity.

For data quality, I do observe some interesting facts. For example, from the result, the top 1 session has 2340 tracks played, and this is within 20 mins!!! This makes me immediately think if the users mouse is problematic and it keeps clicking on the play button to generate tremendous track started records. But this might be intentional for this dataset. If there is a chance to meet with the interviewing committee, I'd really like to have this question asked and discussed.