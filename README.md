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

"" __Code Snippets__

Below is how I created the image of the DAG this program generates. 

![dag screenshot](https://user-images.githubusercontent.com/110640590/205466358-9e3a1952-456d-42da-a720-3e3fd6aa5e3b.png)

Here is an image of the Star Schema that this project creates.

![star schema1](https://user-images.githubusercontent.com/110640590/205466496-7ba9daa9-61ff-4e56-899b-afeea6d0dc15.png)
