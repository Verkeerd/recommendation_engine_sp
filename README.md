# Web-Shop Database Transfer
A repository dedicated to transferring data supporting a web-shop from a mongodb database to a sql database. 
This project is made as an assignment for the course structured programming. 
MongoDB, PostgreSQL and PGAdmin4 are used with this project.

# Contributions
This project is made as an assignment and is not open for contribution outside our project group.
There is no hierarchy in our project group so there are no rules regarding pushing and pulling. 
Please hold yourself to a clean code etiquette when you code for this project.

# Installation
We assume you already have access to the mongodb database and have transferred this to a local mongo server.

Clone this repository.
Install pgadmin. Create a local database and run the file make_webshop_database.ddl with the query tool in pgadmin.
You are now ready use this project.

# Design
The physical datamodel of the SQL web-shop database:
![](../../../../recommendation_database.png)

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

# Logical Framework Content Filtering
The logical framework used to make recommendations. A product is recommended to a user, when it is found in the
recommendation table associated with the profile of that user. A product is in the recommendation table when it is
viewed or is similar to a viewed product.

Propositions:
-
- V(x, y) = x is viewed y
- S(x, y) = x is similar to y
- R(x, y) = x is in the recommendation_table of y
- I(x, y) = x is interesting to y

Formulas
-
Recommending viewed products:
- V(x, c) -> R(x, c)

Recommending products similar to viewed products:
- (V(x, c) ∧ S(x, y)) -> R(y, c)

Recommending viewed and similar products to viewed to the user:
- ((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)

When personal recommendations can't be found, the collaborative logical framework is consulted.

# Logical Framework Collaborative Filtering
The logical framework used to make recommendations. A product is recommended to a user, when similar users have viewed
that product or products similar to it. A similar user is a user with the same customer style. The customer style is
found by assigning them the page type most often associated with the user's sessions.
A product can also be recommended to the user when it has been bought by (all) other users, when no profile type can be
determined.

Propositions:
- 
- V(x, y) = y views x
- S(x, y) = x is similar to y
- R(x, y) = x is in the recommendation_table of y
- I(x, y) = x is interesting to y (product x should be recommended to the user on the website.)
- K(x, y) = y buys x

Formulas
-
Viewed and similar products to viewed are interesting to the user:
- ((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)

Products interesting to a user are interesting to similar users:
- (I(x, c) ∧ S(c, c2)) -> I(x, c2)

Products interesting to a user, based upon similar users interests:
- ((((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)) ∧ S(c, c2)) -> I(y, c2)

Sold products are interesting to all users:
- K(x, c) -> I(x, c) ^ I (y, c)

Discussion
- 
I chose these content-based and collaborative based frameworks because they are very simple to turn into a 
recommendation engine with the available data. I made this decision because I did not have much time. I also only want 
to demonstrate that the database works correctly and the relations can be utilized. These frameworks are enough to 
demonstrate this. However, they (especially the collaborative filtering rule) do not give very good recommendations and 
are not very creative.
