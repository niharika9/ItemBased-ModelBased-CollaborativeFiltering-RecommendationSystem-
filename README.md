# Model Based and Item Based Collaborative filtering Recommender System

## Objectives 
- Task1 : To Implement Model based Collaborative Filtering(CF) recommendation system using Spark MLlib.
- Task 2 : To Implement either a Item-based CF recommendation system without using a library

## Environment Requirements
- Scala 2.11 version
- Spark 2.3.1 version

## DataSet 

Yelp Challenge Dataset. The yelp dataset contains more than 6 million review record between millons of the user 
and business. Because the huge volume and the sparseness between the user and business, the recommendation system 
will take a lot of computation , we extract the subset of the whole dataset contains 452353 reviews between 30,000 user 
and 30,000 business and split them to train data (90%) and test data (10%). Two files in the Data/: train review.csv 
and test review.csv, each file contain three conlumns: user id, business id, and stars. And we will use these two files to finish 
and test our recommendation system.


## Model Based Collaborative Filtering

- Trained the ALS algorithm with the training data and built an ALS Model.
- Used the ALS model to predict the ratings for the test data set.
- ALS does not predict rating for all the test cases due to cold-start problem. I used the average value technique , which is : 
```
If user is already in dataset and item is not 
    get average rating from user ratings vector
Else if item is already in dataset and user is not 
    get average rating from item ratings vector
Else // if both the user and item are new
    give the rating as 2.5
 ```   
### Command to Run the code 

```
Spark-submit --class ModelBasedCF CollabFiltering.jar <train_review.csv> <test_review.csv>
```

### Accuracy and Root Mean Square Error(RMSE)
```
>=0 and <1: 28412 
>=1 and <2: 14100 
>=2 and <3: 2450 
>=3 and <4: 234 
>=4: 40
```
```
RMSE: 1.0891289789406198
```

## Item Based Collaborative Filtering
- Performed Item-Item based Collaborative filtering using Pearson Corelation as the similarity measure for two items 
- Used a near neighbourhood value of 10
- Item-Item based fails when there are no items that are common between users. I.e New user or new item scenario.I
- In such cases , I have used the same average technique like explained in ModelBasedCF
```
if item is already in train dataset and user is not 
    get average rating from item  by users ratings vector
    
```
 
### Command to Run the code 

```
Spark-submit --class ItemBasedCF CollabFiltering.jar <train_review.csv> <test_review.csv>
```

### Accuracy , Root Mean Square Error(RMSE) ,Execution Time
```
>=0 and <1: 29019 
>=1 and <2: 12254 
>=2 and <3: 3017 
>=3 and <4: 830 
>=4: 116
````
```
RMSE: 1.1337658834680784 
```
```
Time: 166.717244539 secs
```




