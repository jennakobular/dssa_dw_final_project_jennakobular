# Final Project for DSSA-5102-001 

## About this project
Within this repo, I create an ETL Pipeline script. The purpose of ETL is to extract data from a database, transform the data, and load it into a new schema --or data warehouse. A star schema of the dvdrental database is created which allows for optimized queries. This is because star schemas have fewer tables and clear join paths. 


## About main1.py

main1.py is the entry point to the ETL pipeline and the program as a whole. 

Within my main1.py I have numerous functions which: 

1. setup(): initializes a database session with DVDRENTAL.
2. extract(): pulls data from DVDRENTAL from SQL queries using a SQLAlchemy connection & pandas.
3. transform(): transforms the data into a pandas df which I can then manipulate. 
4. load(): inserts the transformed data into a new database schema -- which for all intent & purposes is the data warehouse.
5. teardown(): Should close any active sessions and open connections to the database.

## Libraries that are necessary to pip install:
* NetworkX
* Pandas 
* SQLAlchemy 

## __Project Structure__
This project structure should be used to help viewers understand where to "find" things and where to "put" new things. 
*   `.config` - This folder is for configuration files (Example: `.json`, `.yaml`, `.toml`, `.ini`)
*   `.vscode` - This folder is for .json configuration files.
*   `samples` - Shows some examples of how to get started on different aspects of the project.
*   `src` - This is the source code folder containing all application code and modules.
*   `common` - This contains modules that are used in the main code
*   `tools` - A place for automation related scripts.
*   `LICENSE` - Open source license markdown
*   `README` - Markdown file describing the project
*   `requirements.txt` - list of python libraries to install with `pip`
*   `star schema1.png` - this is an image of what the star schema looks like.
*   `dag.png` - an image of what the dag looks like.

## __WorkFlow__
1. Import necessary libraries 
2. Decide what the star schema should look like
3. If using postgreSQL like I am in this project: create a connection instance to the database - I used SQLAlchemy to do so
4. Create a new schema and the necessary tables and columns - I used PGAdmin to do so but it can also be done in code using pypika or even direct SQL queries and commands.
5. Extract data from the public database
6. transform the data to fit the new schema - I used pandas to do so.
7. load the transformed data as dimension tables to the new schema which is our datawarehouse.
8. End the connection to the database.


## __Code Snippets and Images__

Below is how I created the image of the DAG this program generates. 

![dag screenshot](https://user-images.githubusercontent.com/110640590/205466358-9e3a1952-456d-42da-a720-3e3fd6aa5e3b.png)

Here is an image of the Star Schema that this project creates.

![star schema1](https://user-images.githubusercontent.com/110640590/205466496-7ba9daa9-61ff-4e56-899b-afeea6d0dc15.png)


Here is what the DAG looks like:
![dag](https://user-images.githubusercontent.com/110640590/205466522-82fbd5ea-7d9e-4d0c-bf53-1f62c71fa7b2.png)

This function was used to extract data from the DVDRENTAL public schema:
![extract from dvdrental](https://user-images.githubusercontent.com/110640590/205466569-3f67fc43-b3b9-47fe-8fcb-936c38c44ee9.png)

This function was used to load the transformed data into the DSSA schema in PostgreSQL:
![loading dssa schema](https://user-images.githubusercontent.com/110640590/205466597-26c5679a-0179-46f8-95b2-2284f28fcb71.png)

## __How to advance this project__

* The scheduler module could be implemented so that tasks are scheduled to run at a pre-defined interval.
* Additional modules could be added 
* A logger could be added to show the flow of the program 
