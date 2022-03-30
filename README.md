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

V(x, y) = x is viewed y
S(x, y) = x is similar to y
R(x, y) = x is in the recommendation_table of y
I(x, y) = x is interesting to y

A product is in the recommendation_table when it has been viewed before
V(x, c) -> R(x, c)
A product is in the recommendation_table when it is similar to a product that had been viewed before
(V(x, c) ∧ S(x, y)) -> R(y, c)

A product is interesting to the customer when it is in the recommendation_table
((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)

collaborative filtering logical framework

V(x, y) = x is viewed by y
S(x, y) = x is similar to y
R(x, y) = x x is in the recommendation_table of y
I(x, y) = x is interesting to y (product x should be recommended to the user on the website.)

A product is interesting to the customer when it is in the recommendation_table
((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)
A product that is interesting for a customer is also interesting for similar customers
(I(x, c) ∧ S(c, c2)) -> I(x, c2)

A product is interesting for to the customer when it is in the recommendation_table of similar customers
((((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)) ∧ S(c, c2)) -> I(y, c2)

I chose these frameworks because they are very simple to turn into a recommendation engine with the available data. I 
made this decision because I did not have much time. I also only want to demonstrate that the database works correctly 
and the relations can be utilized. These frameworks are enough to demonstrate this. However, they (especially the 
collaborative filtering rule) do not give very good recommendations and are not very creative.

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
