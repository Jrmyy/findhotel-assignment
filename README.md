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

If we have more than one, we take the minimum of distance.

4) Finally, we update the data frame `MedianEnrichedPlaceDF` by putting the value found with the `NeighboursDF`. This operation gives the final data frame `EnrichedPlace`, which will be put in the data warehouse.

**NB:** To put the data frame in the data warehouse, we will use an *upsert* strategy. It is done in 3 steps:

1. We insert the current data frame in a temp table
2. We delete from the usual table all the rows for the `place_id` which are both in the usual and the temp table
3. Then we insert the content of the temp table in the usual table
4. We drop the temp table

### Explaination

### How to run it ?

First you need:
* Scala version 2.11.11
* Apache Spark version 2.2
* sbt 