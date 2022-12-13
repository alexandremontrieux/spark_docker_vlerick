#pip install python-dotenv
#pip install pandas

from pyspark import SparkConf
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.types import *
import os
import pyspark.sql.functions as psf
from pathlib import Path

import pandas as pd
import numpy as np
from numpy import array
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
import statsmodels.api as sm
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import cross_validate
from numpy import mean
from numpy import absolute
from imblearn.over_sampling import SMOTE

load_dotenv()

access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

if access_key is not None:
    print("My secret access key is: ", access_key)
else:
    print("The AWS_SECRET_ACCESS_KEY environment variable is not defined.")


BUCKET = "dmacademy-course-assets"
KEY1 = "vlerick/pre_release.csv"
KEY2 = "vlerick/after_release.csv"

config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Load in the CSV files
pre_release = spark.read.csv(f"s3a://{BUCKET}/{KEY1}", header=True)
after_release = spark.read.csv(f"s3a://{BUCKET}/{KEY2}", header=True)

pre_release.show()
after_release.show()

#Convert to Pandas DataFrames.

prerelease = pre_release.toPandas()
afterrelease = after_release.toPandas()

df_movies = pd.merge(prerelease, afterrelease, how='inner', on= 'movie_title')

delete= ['num_critic_for_reviews', 'gross', 'num_user_for_reviews', 'movie_facebook_likes']
df_movies = df_movies.drop(columns= delete)

df_movies.drop_duplicates(inplace = True)

df_movies['content_rating'] = df_movies['content_rating'].replace(['Not Rated'], 'Unrated')
df_movies['content_rating'] = df_movies['content_rating'].replace(['GP'], 'PG')

genredummies = df_movies['genres'].str.get_dummies(sep= '|')
df_movies = df_movies.join(genredummies)
df_movies = df_movies.drop(columns = 'genres')

content_imputer = SimpleImputer(strategy= "most_frequent") # Set up the strategy for the imputer
content_imputer.fit(df_movies[['content_rating']]) # We 'train' the content imputer to know what the most frequent value is for content rating
df_movies['content_rating'] = content_imputer.transform(df_movies[['content_rating']]) # We transform all the null values to the mode

df_movies = df_movies.dropna()

genrelist = genredummies.columns.values.tolist()
othergenreslist= []

for i in genrelist:
    if df_movies[df_movies[i] == 1].shape[0] < 30:
        othergenreslist.append(i)


othergenres = []

for i in range(0, df_movies.shape[0]):
    for j in othergenreslist:
        if df_movies.iloc[i, df_movies.columns.get_loc(j)] == 1:
            othergenres.append(1)
            break
    else:
        othergenres.append(0)

df_movies['Othergenres'] = othergenres
df_movies = df_movies.drop(columns= othergenreslist)

languagedummies = pd.get_dummies(df_movies['language'])
languagelist = languagedummies.columns.values.tolist()
df_movies = df_movies.join(languagedummies)
df_movies = df_movies.drop(columns = 'language')

otherlanguageslist= []

for i in languagelist:
    if df_movies[df_movies[i] == 1].shape[0] < 50:
        otherlanguageslist.append(i)


df_movies = df_movies.drop(columns= otherlanguageslist)

countrydummies = pd.get_dummies(df_movies['country'])
countrylist = countrydummies.columns.values.tolist()
df_movies = df_movies.join(countrydummies)
df_movies = df_movies.drop(columns = 'country')

othercountrieslist= []

for i in countrylist:
    if df_movies[df_movies[i] == 1].shape[0] < 100:
        othercountrieslist.append(i)


df_movies = df_movies.drop(columns= othercountrieslist)
df_movies = df_movies = df_movies.drop(columns= ['actor_1_name', 'actor_2_name', 'actor_3_name', 'director_name', 'movie_title'])

contentdummies = pd.get_dummies(df_movies['content_rating'])
contentlist = contentdummies.columns.values.tolist()
df_movies = df_movies.join(contentdummies)
df_movies = df_movies.drop(columns = 'content_rating')

othercontentlist= []

for i in contentlist:
    if df_movies[df_movies[i] == 1].shape[0] < 100:
        othercontentlist.append(i)


othercontent = []

for i in range(0, df_movies.shape[0]):
    for j in othercontentlist:
        if df_movies.iloc[i, df_movies.columns.get_loc(j)] == 1:
            othercontent.append(1)
            break
    else:
        othercontent.append(0)

df_movies['Othercontent'] = othercontent

df = df_movies.copy()

df_for = df
label = []

df_for['imdb_score'] = pd.to_numeric(df_for['imdb_score'])

for i in range(0, df_for.shape[0]):
    if df_for.iloc[i,df_for.columns.get_loc('imdb_score')] < 7:
        label.append(0)
    else:
        label.append(1)

df_for['Label'] = label
df_for = df_for.drop(columns= 'imdb_score')

x = df_for.drop(columns= 'Label')
y = df_for['Label']
x_train, x_val, y_train, y_val = train_test_split(x,y,test_size=0.3, random_state = 1)
#sm = SMOTE(sampling_strategy='minority', random_state=1)
#X_res, y_res = sm.fit_resample(x_train, y_train)
regressor = RandomForestClassifier(n_estimators= 100, random_state= 1) # We use 100 decision trees to build our forest
model= regressor.fit(x_train, y_train)
y_predict_train = model.predict(x_train)
y_predict = model.predict(x_val)

print(y_predict)


df_spark = pd.DataFrame(y_predict)

spark_df = spark.createDataFrame(df_spark)
spark_df.write.json(f"s3a://{BUCKET}/vlerick/alexandre_montrieux")



