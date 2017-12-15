# FindHotel - Data Engineer Application

## My Data Engineer Challenge

The biggest challenge I faced during my journey with Big Data tools was when I was an intern at Datadog. In fact, one of our mission was to give a full presentation of the marketing performance of the company to the board meeting. Nevertheless, this marketing team I joined was very new (only a couple of months old), so we did not have any full pipeline system, with a filled data warehouse. Moreover, all the team, including me, was beginning with Apache Spark, and more generally, with Big Data tools. So, during a full month, we dive deep into all the technologies of pipelining, Big Data, Data Warehouse in order to be as performant as possible. We worked from 9am to 10pm during this period in order to give, at the end, a full presentation of the marketing performance of the company for the board meeting. Thanks to this intensive month I was able to:

- Increase my skills concerning Apache Spark, Amazon Web Services (S3, EC2, Redshift), Pipeline tools such as Luigi or Airflow
- Provide a full pipeline which deals with several terabytes of data, from several sources (Postgres, S3, Marketo, Salesforce, Google, ...), clean this data using Apache Spark, aggregate it in order to give meaningfull KPIs, store it in a data warehouse in order to use a dashboarding tool to create the report
- Understand how powerfull a data-driven company can be
- Worker under (a lot of) pressure

## The assignement

You can find in this project 2 implementations solutions, one working with Spark, and the other one using `pandas`library with Airflow pipelining system.

### The logic

Both solutions use the following system:

1) First we get the list of all the parents for one `place_id` using the `navigation_path`. So for this first step, we only use the `Place`table. For instance

|place_id|navigation_path|
|--------|---------------|
|22      |/22/           |
|45      |/22/45/        |
|100     |/22/45/100/    |
|200     |/44/200/       |

becomes

|place_id|parent_place_id|
|--------|---------------|
|22      |22             |
|45      |22             |
|45      |45             |
|100     |22             |
|100     |45             |
|100     |100            |
|200     |44             |
|200     |200            |

2) Then we join this new data frame with the `Hotel` table, by joining on the `place_id` of the 2 tables. Then we just have to group by `parent_place_id` to get the median of the list of the `nightly_rate` of the hotels, which is the `representative_nightly_price`. This enables us to have for every place all the hotels in the place, but also in all the children places. So, if we take our previous example, for place 22, we will have the hotels of place 22, 45 and 100. This creates the `MedianEnrichedPlaceDF`

**NB:** This only fills 17% of the `representative_nightly_price`. In order to fill in more places, we are going to add a new approach, a complement of the first one.

3) We are going to create a new data frame `NeighboursDF`. It is built with the data frame we created just before, with the median calculation. First we split this data frame into 2 dataframes, one with `place_id` which have a `representative_nightly_price`, the other with places without this data. Then we attribute the `representative_nightly_price` of the `place_id` which satisfies 2 conditions:

- This `place_id` has a non-null `representative_nightly_price`
- This `place_id` is distant by less than 10 kms

If we have more than one, we take the median of all the `representative_nightly_price`.

4) Finally, we update the data frame `MedianEnrichedPlaceDF` by putting the value found with the `NeighboursDF`. This operation gives the final data frame `EnrichedPlace`, which will be put in the data warehouse.

**NB:** To put the data frame in the data warehouse, we will use an *upsert* strategy. It is done in 3 steps:

1. We insert the current data frame in a temp table
2. We delete from the usual table all the rows for the `place_id` which are both in the usual and the temp table
3. Then we insert the content of the temp table in the usual table
4. We drop the temp table

**NB:** In local we won't do the upsert, but we will only store the data in a sqlite database.

### Justification

I think my solution offers a good compromise between accuracy et efficiency. In fact, by calculing the median of the hotels present in the place, we take the value that represents the most the night price for this place. By getting also all the hotels of the children places, we are just saying that an hotel in a place is also in all its parents. This was not done in the current `Hotel`table.

One idea I had when trying to find the `representative_nightly_price` for the places without hotels, I thought about using the different attributes of each place (place type, place category, place group) and try to create a model to predict the representative nightly price. Nevertheless, I realized that the most important attribute when someone is looking for an hotel is where it is. We cannot compare an hotel in the historical area of Paris with an hotel in the historical area of Amsterdam, but we surely compare hotels in all Paris (or at least in a circle around a "perfect spot"). So I took 10 kilometers as the radius from the "perfect spot" and I applied this logic to say that places without known hotels have a price similar to the places near them (in less than 10 kilometers).

I think these approachs are efficient because there rely on pragmatism, on facts, and not on a model with parameters for which it is hard to tell if there are really important.

### Piece as part of a bigger ETL project

Several alternatives can be offered in order to deploy it to AWS and to put it as a part of a bigger ETL pipeline. In fact, first, if you decide to keep the job as it is, you can put it in AWS ElasticMapReduce, which gives the advantage of scheduling the launch of Spark jobs. The drawback of this method is that it will be isolated of the rest of the pipeline. The other solution, the one I used when I was at Datadog, is to use a pipeline tool like Airflow or Luigi. You can add a new task for this Spark job, that will be triggered after the required tasks, before this one, are fullfilled.

In order to monitor the pipeline, you can use the AWS CloudWatch or an external tool such as Datadog.

### How to run it ?

First you need:
* Scala version 2.11.11
* Apache Spark version 2.2
* sbt 

Then you just need to cd into the folder, and run these 2 commands:

1. `sbt assembly`
2. `spark-submit --class findhotel.compute.EnrichedPlacePopuler --master "local[4]" --conf spark.is_local=true target/scala-2.11/findhotel-assembly-0.1.jar`
3. You can find the content of the `EnrichedPlace` in the sqlite database by typing `sqlite3 findhotel.db`

### Limitations

This job, on my local computer, takes more than one hour to complete for the full dataset. For 70% of the dataset, it takes about 40 minutes to fill in the `EnrichedPlace` table in data warehouse.
