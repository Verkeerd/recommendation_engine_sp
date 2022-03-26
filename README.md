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

logical →
p = Product is recommended to this profile
q = Product is interesting for the customer

p → q

q = product 


# Contributions
This project is made as an assignment and is not open for contribution outside our project group.
There is no hierarchy in our project group so there are no rules regarding pushing and pulling. 
Please hold yourself to a clean code etiquette when you code for this project.
 
