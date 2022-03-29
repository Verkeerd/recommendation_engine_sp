# Web_shop_database_transfer
A repository dedicated to transferring data supporting a web-shop from a mongodb database to a sql database. 
This project is made as an assignment for the course structured programming. 
MongoDB, PostgreSQL and PGAdmin4 are used with this project.

# Installation
We assume you already have access to the mongodb database and have transferred this to a local mongo server.

Clone this repository.
Install pgadmin. Create a local database and run the file make_webshop_database.ddl with the query tool in pgadmin.
You are now ready use this project.

# Design
The physical datamodel of the SQL web-shop database:

![](../../Documents/VP Projects/recommendation_database.png)

I selected the data on basis of their potential usefulness; I.E. Do I think this data will help me make a decision on 
what products to recommend to users? 
I made this choice because, with all the  data already available, I can test which logical frameworks are best without 
having to change the data-transfer between designs. When a framework has been decided upon, a new data-selection will be 
made.

The data is loaded into python from mongoDB, document by document to preserve memory.
The documents are scanned for the data selection and is inserted into the sql database row by row. There are no commits 
in between the inserts, only one commit at the end.

I chose the separate insert statements because it was the first way I thought of, but it is not very fast. 
I want to make one big insert statement, maybe compress it, and execute it in one go. However, this has not been 
implemented yet.

# Logical Framework
content filtering logical framework:

p = Product is recommended to the customer
q = Product is interesting to the customer

p → q

collaborative filtering logical framework

r(x, y) = x is recommended to y (product x is recorded under recommended_products under y)
i(x, y) = x is interesting to y (product x should be recommended to the user on the website.)
s(x, y) = x is similar to y (profiles x and y have the same segment in recommendation)
r = product is interesting for the customer

(r(x, y) → i(x, y) /\ r(y, z)) → i(x, z) 

I chose these frameworks because they are very simple. I chose for this, because I did not have much time and I only 
want to demonstrate that the database works correctly and the relations can be utilized. These frameworks are enough to 
demonstrate this. However, they (especially the collaborative filtering rule) do not give very good recommendations.

# TODO
speed up the data transfer:
- construct ddl file to insert all data in one go. 
- research compression of ddl file before send off.
- add ranking between products
- add bought_products to recommendations
- add better profiles for users

# Contributions
This project is made as an assignment and is not open for contribution outside our project group.
There is no hierarchy in our project group so there are no rules regarding pushing and pulling. 
Please hold yourself to a clean code etiquette when you code for this project.
