"""
This script serves to generate several datasets that are uploaded to both a relational database solution (MySQL) and NoSQL data store (MongoDB).
The data is randomly generated using the random and Faker packages. It is then transformed to a dataframe object and inserted into the 
appropriate table or collection. 
"""


import pandas as pd
from collections import defaultdict

import mysql.connector
import pymysql
from sqlalchemy import create_engine
from pymongo import MongoClient

import random
from faker import Faker


# MySQL connection data
conn = mysql.connector.connect(
    host='localhost',
    user='root'
)
cursor = conn.cursor()
engine = create_engine()  # Connection string

# MongoDB connection data
client = MongoClient()  # Connection string
db = client["movies"]

fake = Faker()
Faker.seed(1996)
random.seed(1996)

try:
    cursor.execute("CREATE DATABASE movies")
except Exception as e:
    print(e)
    
create_table_movies = """
CREATE TABLE movies.movies (
    movie_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(300) NOT NULL,
    country VARCHAR(100),
    year INT NOT NULL
);
"""

create_table_users = """
CREATE TABLE movies.users (
    user_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name varchar(50) NOT NULL,
    last_name varchar(50) NOT NULL,
    most_watched_movie INT
);
"""

create_table_ratings = """
CREATE TABLE movies.ratings (
    rating INT,
    user_id INT NOT NULL,
    movie_id INT NOT NULL,
    CHECK (rating BETWEEN 0 AND 100)
);
"""

create_table = [
    create_table_movies,
    create_table_users,
    create_table_ratings
]

for i in create_table:
    try:
        cursor.execute(i)
    except Exception as e:
        print(e)

movies = defaultdict(list)
users = defaultdict(list)
ratings = defaultdict(list)

# Add fake generated data to the dictionaries
for _ in range(10000):
    movies["movie_id"].append(_+1)
    movies["title"].append(fake.text(max_nb_chars=20).title()[0:-1])
    movies["country"].append(fake.country())
    movies["year"].append(fake.year())

for _ in range(10000):
    users["user_id"].append(_+1)
    users["first_name"].append(fake.first_name())
    users["last_name"].append(fake.last_name())
    users["most_watched_movie"].append(random.randint(0,10000))

for _ in range(10000):
    ratings["rating"].append(random.randint(0,100))
    ratings["user_id"].append(random.randint(0,10000))
    ratings["movie_id"].append(random.randint(0,10000))
 
# Create dataframe objects from dictionaries
movies_df = pd.DataFrame(movies)
users_df = pd.DataFrame(users)
ratings_df = pd.DataFrame(ratings)

# Add records to MySQL tables
try:
    movies_df.to_sql(
        name='movies',
        con=engine,
        if_exists='append',
        index=False
    )

    users_df.to_sql(
        name='users',
        con=engine,
        if_exists='append',
        index=False
    )

    ratings_df.to_sql(
        name='ratings',
        con=engine,
        if_exists='append',
        index=False
    )
except Exception as e:
    print(e)
    
# Add documents to MongoDB collections
db.movies.insert_many(movies_df.to_dict('records'))
db.users.insert_many(users_df.to_dict('records'))
db.ratings.insert_many(ratings_df.to_dict('records'))

test_query = """
SELECT * 
FROM movies.movies
LIMIT 10;
"""

conn.cursor().execute(test_query)
result = cursor.fetchall()

for i in result:
    print(i)
