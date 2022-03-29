# Web_shop_database_transfer
A repository dedicated to transferring data supporting a web-shop from a mongodb database to a sql database. 
This project is made as an assignment for the course structured programming. 
MongoDB, PostgreSQL and PGAdmin4 are used with this project.

# Installation
We assume you already have access to the mongodb database and have transferred this to a local mongo server.

Clone this repository.
Install pgadmin. Create a local database and run the file make_web_shop_simple.ddl with the query tool in pgadmin.
You are now ready use this project.

# Design
The physical datamodel of the SQL web-shop database:

![](../../Documents/VP Projects/recommendation_database.png)

I selected the data on basis of their potential usefulness; I.E. Do I think this data will help me make a decision on 
what products to recommend. I made this choice because with all the  data already available, I can test which logical
frameworks are best without having to change the data-transfer between designs.
When a framework has been decided upon, a new data-selection will be made.

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

# TODO
speed up the data transfer:
- construct ddl file to insert all data in one go. 
- research compression of ddl file before send off.

# Contributions
This project is made as an assignment and is not open for contribution outside our project group.
There is no hierarchy in our project group so there are no rules regarding pushing and pulling. 
Please hold yourself to a clean code etiquette when you code for this project.
 
