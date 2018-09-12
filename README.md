If you have experience of a regular RDBMS management system like Oracle or SQL Server you might at some stage have come across 
so-called analytic or windowing functions. Analytics allow you to calculate aggregates and rankings for groups of rows within 
your dataset. They are extremely useful and allow SQL to do things that are difficult if not impossible without them. The good 
news is that analytic functions have also been available to users of Spark-SQL for some time.

The general format of an analytic function is:-

# analytic_function over(partition_by_clause  order_by_clause   windowing_clause)

Let’s look at each of these in turn.

# analytic_function

There are a number of these including SUM, COUNT, AVG, LAG, LEAD, FIRST_VALUE. But we concentrate on the RANK, DENSE_RANK and 
ROW_NUMBER ranking functions in this article.

# over

The over keyword is required and indicates the starting of the grouping definition. An empty over() clause means you want to 
group over the entire data set.

# partition_by_clause

Here is where you state which columns (one or more) of your dataset you want to group on.

# Order_by_clause

Define how the columns that make up the grouping are to be ordered.

# windowing_clause

Within a grouping defines a range of records we want the analytic_function to operate on with respect to the current row. There 
are two different types, a row window i.e a number of physical rows to look back and/or forward from the current record and a 
range window which defines a value to subtract or add to the value of the current row to define a range or rows. We don’t need
to concern ourselves with the windowing clause just now as the ranking analytic functions that we’re going to discuss don’t
actually use it.

Analytics are kind of hard to explain just using words so the best bet is to actually show you some examples. In this article
I’m concentrating on how you use the three most common analytic ranking functions:- rank, dense_rank and row_number. For our 
sample data set we just set up a python list of tuples of stock data for 4 top US companies. We store their name, price,
price date and volume over 4 days.

All the examples below were run on a Windows PC using a Jupyter notebook running pyspark.

```
###First, some initialisation code

import findspark

findspark.init()

import pyspark

import datetime

sc = pyspark.SparkContext(appName="read-big-file")

from pyspark.sql import SparkSession

spark = SparkSession \

    .builder \

    .getOrCreate()

from pyspark.sql.types import *
 
# Our schema for the data
#

dbSchema = StructType([\

StructField("stock", StringType()),\

StructField("price",DoubleType()),\

StructField("date",StringType()),\

StructField("volume",LongType())])

# Our actual data
#

db_list = [\

('IBM',143.91,'2018-08-15',4241500),\

('IBM',145.34,'2018-08-16',5250700),\

('IBM',146.06,'2018-08-17',2678600),\

('IBM',146.38,'2018-08-20',3250700),\

('MSFT',107.66,'2018-08-15',29982800),\

('MSFT',107.64,'2018-08-16',21384300),\

('MSFT',107.58,'2018-08-17',18053800),\

('MSFT',106.65,'2018-08-20',6127595),\

('AAPL',210.24,'2018-08-15',28807600),\

('AAPL',213.32,'2018-08-16',28500400),\

('AAPL',217.58,'2018-08-17',35050600),\

('AAPL',215.80,'2018-08-20',16455456)\

]

# Convert our list of tuples to a dataframe
#

df = spark.createDataFrame(db_list,schema=dbSchema) 

# Convert the date string to an actual date
#

df=df.withColumn("date",df["date"].cast(DateType()))

df.show()

+-----+------+----------+--------+
|stock| price|      date|  volume|
+-----+------+----------+--------+
|  IBM|143.91|2018-08-15| 4241500|
|  IBM|145.34|2018-08-16| 5250700|
|  IBM|146.06|2018-08-17| 2678600|
|  IBM|146.38|2018-08-20| 3250700|
| MSFT|107.66|2018-08-15|29982800|
| MSFT|107.64|2018-08-16|21384300|
| MSFT|107.58|2018-08-17|18053800|
| MSFT|106.65|2018-08-20| 6127595|
| AAPL|210.24|2018-08-15|28807600|
| AAPL|213.32|2018-08-16|28500400|
| AAPL|217.58|2018-08-17|35050600|
| AAPL| 215.8|2018-08-20|16455456|
+-----+------+----------+--------+
```

It’s possible to use the analytic functions directly on the dataframe but as I’m used to things from an RDBMS point of view 
I’m going to convert our dataframe to a table (view) so I can run more natural SQL on it.

```
###convert our dataframe to a “view”
#

df.createOrReplaceTempView("stock")
```

Now that we have our table we can start to use our ranking functions. Note that all ranking analytics need an order by clause.

# Rank

The rank function allows you to assign a sequential number to rows. Rank sequence numbers are not necessarily consecutive. 
The best way to think of it is to imagine runners in an Olympic race, say. If there is a dead heat for first place the two 
athletes are assigned first position and win gold. The next person home, though, is not assigned second position, they will 
be awarded third position (the bronze medal).  

```
Sql.Context.sql(‘select stock, rank() over(order by stock) rnk  from stock’).show()

+-----+---+
|stock|rnk|
+-----+---+
| AAPL|  1|
| AAPL|  1|
| AAPL|  1|
| AAPL|  1|
|  IBM|  5|
|  IBM|  5|
|  IBM|  5|
|  IBM|  5|
| MSFT|  9|
| MSFT|  9|
| MSFT|  9|
| MSFT|  9|
+-----+---+
```

Let’s try a little more realistic example now. Bring out the data where the stocks had the most volume traded

```
sqlContext.sql('select * from (SELECT stock,price,volume,date,rank() over(partition by stock order by volume desc ) \
as rnk FROM stock') where rnk = 1).show()

+-----+------+--------+----------+---+
|stock| price|  volume|      date|rnk|
+-----+------+--------+----------+---+
| AAPL|217.58|35050600|2018-08-17|  1|
|  IBM|145.34| 5250700|2018-08-16|  1|
| MSFT|107.66|29982800|2018-08-15|  1|
+-----+------+--------+----------+---+
```


Let’s determine the rank of the highest price stocks each day with the highest price ranked first.

```
sqlContext.sql('SELECT stock,price,volume,date,rank() over(partition by date order by price desc ) as rnk FROM stock').show()

+-----+------+--------+----------+---+
|stock| price|  volume|      date|rnk|
+-----+------+--------+----------+---+
| AAPL|213.32|28500400|2018-08-16|  1|
|  IBM|145.34| 5250700|2018-08-16|  2|
| MSFT|107.64|21384300|2018-08-16|  3|
| AAPL|217.58|35050600|2018-08-17|  1|
|  IBM|146.06| 2678600|2018-08-17|  2|
| MSFT|107.58|18053800|2018-08-17|  3|
| AAPL|210.24|28807600|2018-08-15|  1|
|  IBM|143.91| 4241500|2018-08-15|  2|
| MSFT|107.66|29982800|2018-08-15|  3|
| AAPL| 215.8|16455456|2018-08-20|  1|
|  IBM|146.38| 3250700|2018-08-20|  2|
| MSFT|106.65| 6127595|2018-08-20|  3|
+-----+------+--------+----------+---+
```

# Dense_rank

The dense_rank function is similar to the rank function in that it also allows you to assign a sequential number to rows. 
The difference is that dense_rank guarantees consecutive number sequences with no gaps. Continuing our Olympic race scenario, 
under dense_rank conditions the two dead heaters would still win gold but the next person home would be deemed in second place 
and get a silver medal. Substituting in dense_rank with the normal rank in our first ranking SQL above we get this:

```
sqlContext.sql('SELECT stock,dense_rank() over(order by stock) as rnk FROM stock').show()

+-----+---+
|stock|rnk|
+-----+---+
| AAPL|  1|
| AAPL|  1|
| AAPL|  1|
| AAPL|  1|
|  IBM|  2|
|  IBM|  2|
|  IBM|  2|
|  IBM|  2|
| MSFT|  3|
| MSFT|  3|
| MSFT|  3|
| MSFT|  3|
+-----+---+
```

# row_number

The row_number analytic simply assigns each row within a partition a non-deterministic unique interger numeric value. On the face 
of it this sounds like the other two ranking functions but the key difference is that “ties” within a partition are assigned 
different ranks, and each row number within a partition is unique and gap-free. Using our Olympic race scenario, one of the dead
heaters would win gold, the other would win silver and the next person home would win bronze. Here's an example:

```
sqlContext.sql('SELECT stock,price,volume,date,row_number() over(order by stock) as rn FROM stock').show()

+-----+------+--------+----------+---+
|stock| price|  volume|      date| rn|
+-----+------+--------+----------+---+
| AAPL|210.24|28807600|2018-08-15|  1|
| AAPL|213.32|28500400|2018-08-16|  2|
| AAPL|217.58|35050600|2018-08-17|  3|
| AAPL| 215.8|16455456|2018-08-20|  4|
|  IBM|143.91| 4241500|2018-08-15|  5|
|  IBM|145.34| 5250700|2018-08-16|  6|
|  IBM|146.06| 2678600|2018-08-17|  7|
|  IBM|146.38| 3250700|2018-08-20|  8|
| MSFT|107.66|29982800|2018-08-15|  9|
| MSFT|107.64|21384300|2018-08-16| 10|
| MSFT|107.58|18053800|2018-08-17| 11|
| MSFT|106.65| 6127595|2018-08-20| 12|
+-----+------+--------+----------+---+
```

Note that because it is non-deterministic, if you ran the above same query again there is a chance that the same records 
could have different rankings.

A common use of row_number() is to de-duplicate a table of data. Now, dataframes have their own built in de-duplicate function, 
so let’s see which one, if any, is faster. First of all we create a bit more data to play with. I’m simply going to unionAll 
our original data frame a few times to build up the rows , 3072 in total.

```
###Make a copy of our original dataframe that had 12 records
 
df2 = df

for i in range(8):

    df2=df2.unionAll(df2)

    df2.cache() 

df2.count()

 
# Above count returns 3072 records 

# Create a new view on the enlarged dataframe
 

df2.createOrReplaceTempView("stock2")
```

Now try de-duplicating both, one using the built-in dataframe function and the other using sql and # row_number()

# The Dataframe Method

```
print(datetime.datetime.now())

df3=df2.dropDuplicates()

df3.show()

print(datetime.datetime.now())

2018-08-21 13:02:02.384703

+-----+------+----------+--------+
|stock| price|      date|  volume|
+-----+------+----------+--------+
|  IBM|143.91|2018-08-15| 4241500|
| MSFT|106.65|2018-08-20| 6127595|
|  IBM|146.06|2018-08-17| 2678600|
| MSFT|107.58|2018-08-17|18053800|
| AAPL| 215.8|2018-08-20|16455456|
| AAPL|217.58|2018-08-17|35050600|
| MSFT|107.66|2018-08-15|29982800|
| MSFT|107.64|2018-08-16|21384300|
|  IBM|145.34|2018-08-16| 5250700|
|  IBM|146.38|2018-08-20| 3250700|
| AAPL|210.24|2018-08-15|28807600|
| AAPL|213.32|2018-08-16|28500400|
+-----+------+----------+--------+ 
 

2018-08-21 13:02:42.789753
```

# Now using the SQL analytics method

```
print(datetime.datetime.now()) 

stock_tmp=sqlContext.sql('select stock,price,volume,date from (SELECT stock,price,volume,date,row_number() \
over(partition by stock, date order by date desc ) as rn FROM stock2) where rn =1 ')

stock_tmp.show() 

print(datetime.datetime.now())

2018-08-21 13:12:21.377016

+-----+------+--------+----------+
|stock| price|  volume|      date|
+-----+------+--------+----------+
| AAPL|213.32|28500400|2018-08-16|
|  IBM|146.38| 3250700|2018-08-20|
| AAPL|210.24|28807600|2018-08-15|
| MSFT|107.58|18053800|2018-08-17|
| AAPL|217.58|35050600|2018-08-17|
|  IBM|145.34| 5250700|2018-08-16|
| AAPL| 215.8|16455456|2018-08-20|
| MSFT|107.66|29982800|2018-08-15|
|  IBM|143.91| 4241500|2018-08-15|
|  IBM|146.06| 2678600|2018-08-17|
| MSFT|106.65| 6127595|2018-08-20|
| MSFT|107.64|21384300|2018-08-16|
+-----+------+--------+----------+
 

2018-08-21 13:12:58.314197
```

Hmmm interestingly, the SQL analytic method just squeaks it, 37 seconds v 40 seconds.
