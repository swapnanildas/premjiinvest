import os 
import pandas as pd
import numpy as np
import pandasql as ps
import sqlalchemy



'''
Reading user demography data 

'''
user = pd.DataFrame(columns = ['userid', 'age' , 'gender' , 'occupation', 'zipcode'])
f = open("ml-100k/u.user", "r")
for x in f:
  user = pd.concat([user, pd.DataFrame([x.split('|')],columns = ['userid', 'age' , 'gender' , 'occupation', 'zipcode'])],ignore_index=True)
user['zipcode'] = [ x.rstrip("\n") for x in user['zipcode'] ] # will produce "Hello world! \r"
user['age'] = [ int(x) for x in user['age'] ]


'''
Fetching the survey data 
'''
data = pd.read_csv("ml-100k/u.data",delimiter="\t",names= ['userid' , 'itemid' , 'rating' , 'timestamp'])




aggregations = {
    # work on the "duration" column
    'age': { 
        # get the sum, and call this result 'total_duration'
        'total_duration': 'sum',  
        # get mean, call result 'average_duration'
        'average_age': 'mean', 
    }
}


'''
1. Getting the average age of user by occupation
'''


avg_ages = user.groupby('occupation').agg({'age':'mean'}).rename(columns={'age': 'avg_age'}).reset_index()


'''
2. Names of top 20 highest rated movies. (at least 35 times rated by Users)
'''

columns= ['movieid' ,'movietitle' ,'releasedate' ,'videoreleasedate' ,'IMDbURL' ,'unknown' ,'Action' ,'Adventure' ,'Animation' ,"Children's" ,'Comedy' ,'Crime' ,'Documentary' ,'Drama' ,'Fantasy' ,'Film-Noir' ,'Horror' ,'Musical' ,'Mystery' ,'Romance' ,'Sci-Fi' ,'Thriller' ,'War' ,'Western']



item = pd.DataFrame(columns = columns)
genres = ['unknown' ,'Action' ,'Adventure' ,'Animation' ,"Children's" ,'Comedy' ,'Crime' ,'Documentary' ,'Drama' ,'Fantasy' ,'Film-Noir' ,'Horror' ,'Musical' ,'Mystery' ,'Romance' ,'Sci-Fi' ,'Thriller' ,'War' ,'Western']
f = open("ml-100k/u.item", "r")
for x in f:
  item = pd.concat([item, pd.DataFrame([x.split('|')],columns = columns)],ignore_index=True)
item['Western'] = [ x.rstrip("\n") for x in item['Western'] ]
item['movieid'] = [ int(x) for x in item['movieid'] ]
for genre in genres:
    item[genre] = [ int(x) for x in item[genre] ]

q2 = """
with cte as (
select b.movietitle
,sum(a.rating) as sum_rating
,count(a.rating) as count_rating
from data a 
left join item b
on a.itemid = b.movieid
group by 1)

select movietitle 
,sum_rating*1.0/count_rating as avg_rating
from cte 
where count_rating>=35
order by 2 desc 
limit 20
"""

top_movies = ps.sqldf(q2, locals())


'''

3. Top genres rated by users of each occupation in every age-groups. age-groups can be defined as 20-25, 25-35, 35-45, 45 and older

'''
def get_genre(row):
    genre = []
    for c in genres:
        if row[c]==1:
            genre.append(c)
    return genre
item['all_genres'] = item.apply(get_genre, axis=1)
item_xplode = item.explode('all_genres')


q3 = """
with users as (
select userid 
,case when age between 20 and 25 then '25 to 25'
when age between 26 and 35 then '26 to 35'
when age between 36 and 45 then '36 to 45'
when age > 45 then '45 and older' end as agegroups
,occupation
from user
where agegroups is not null
)
,cte as (
select b.agegroups
,b.occupation
,c.all_genres as genre 
,sum(a.rating)*1.0/count(a.rating) as avg_rating
from data a 
left join users b 
on a.userid = b.userid 
left join item_xplode c 
on a.itemid = c.movieid
group by 1,2,3
)
,cte2 as (
select agegroups 
,occupation
,genre
,avg_rating
,row_number() over(partition by occupation, agegroups order by avg_rating desc) rnk
from cte 
)

select agegroups
,occupation
,genre
,avg_rating
from cte2
where rnk=1
"""

topgenres = ps.sqldf(q3, locals())

'''
4. The similarity calculation can change according to the algorithm.

'''



