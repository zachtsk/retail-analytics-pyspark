# Retail Analytics with PySpark
Spark-ML notebooks for clustering retail stores, forecasting sales, and learning price elasticities.

# Getting Started

To use this codebase, you'll [need to have Docker installed](https://docs.docker.com/docker-for-windows/install/). After installing Docker, you can get started by running `docker-compose up -d --build`

From there, you can navigate to http://localhost:8889/tree/notebooks to check out the different example notebooks.

Once you're finished and you want to spin everything down `docker-compose down`

# Insights
Spark is a great tool for performing memory or compute-heavy data transformations. For data science applications, it also simplifies the process of training of large ensemble models or creating many individual models in parallel. In this example, thousands of individual store_cluster+product linear regression models are trained using the following features: `price`,`store_display_flag`,`store_feature_flag`,`tpr_flag`

In fast-moving consumer retail, it's not uncommon for these few key features to capture a strong signal in predicting how well an item will sell.

Here are a few example predictions for top-selling products:

![brave_F89paTD7id](https://user-images.githubusercontent.com/109352381/204587757-4c3f6ab8-70e1-4a78-953d-a0febf3c46bd.png)

# Project Structure

    retail-pyspark              
       ├── datalake                       # Usually this dir would be gitignored. Don't save flat files in git. 
       |    └── dh_transactions.csv.gz    
       ├── notebooks                      # Pyspark Notebooks
       ├── grocery  
       |    ├── abstract                  # For abstract classes to be inherited
       |    |    └── entity.py            
       |    ├── config                    # For various configuration files
       |    |    └── dev.yml              
       |    ├── jobs                      # For various tasks+jobs to be executed
       |    |    ├── make_store_clusters.py
       |    |    ├── make_model_data.py
       |    |    ├── make_model_training.py
       |    |    └── standardize_files.py
       |    ├── pipelines                 # For pipelines, which essentially chain jobs together
       |    ├── utils                     # For helper functions and code which is not fleshed out enough to be 
       |    |    ├── config.py            #  its own module
       |    |    ├── io.py
       |    |    └── log.py
       |    └── main.py                   # Code entrypoint
       ├── scripts                         
       |    └── rebuild_package.sh        # Script to be run in Docker container to re-install custom library
       ├── tests                          # Unit & Integration testing
       |    ├── test_config.py     
       |    ├── test_io.py
       |    └── test_pyspark_init.py            
       ├── docker-compose.yml
       ├── Dockerfile
       └── Makefile                       # `make` command shortcuts

# Design Patterns used in this repo

#### A few notes about the folder structure:
* __Abstract__ contains interfaces to be inherited elsewhere (primarily by __Jobs__ or __Operators__). Usually containing some boilerplate functionality and defining required methods (e.g. `run()` method). 
* __Jobs__ are where all of the core data transformations take place. These are used for managing data & models.
* __Pipelines__ organize and run __Jobs__
* __Utils__ help support __Jobs__. These are re-usable functions.
* __Config__ are YAML files containing project inputs. (does not include any sensitive data/passwords/etc)  

#### Other items
* __Scripts__ contain container-level code used to manage the execution environment. For example, there is a script used for re-building the `grocery` python package that is used throughout the repository.
* __Tests__ contain unit + integration tests. 


## Jobs inherit the Entity class

Within `grocery.abstract` there is a general `Entity` class that can be extended to manage config information, spark instances, and utilize some helper methods.

It must be initialized with at least a __spark instance__, and a __config dictionary__. 

## Jobs all have a `run` method

In this repo, the convention for each job is that there is a `run` method, along with a `run_safe` method which executes all dependencies first.

## PySpark and UDFs conventions

This repo uses a few specific PySpark conventions:
* Generally default to using `F.expr(<spark sql>)` for column calculations
* When performing joins:
  1. Left side of the join should be the source data
  2. The Right side dataset "joining columns"  are __identically named to it's matching columns in the source data__.
  3. The Right side dataset __only includes explicitly selected new columns__ (use a `select` method before joining). Anything that might be duplicated or not being used should be dropped before joining 
* Use list unpacking within grouping + aggregating commands e.g. `df.groupby(*group_ls).agg(*agg_ls)`
* Utilize **config_file** for data source io info, including schema info. Using helper functions `read_spark_data` and `write_spark_data`.
* Try to separate job into a series of logical datasets, each being created by a single purpose function. 
* Some naming conventions for methods I generally use:
  - `def make_xyz(df)`: Method takes single dataframe param, performs transformation, returns modified dataframe
  - `def produce_xyz(df_a, df_b)`: Method takes multiple dataframes, performs joins/tranformations, returns new blended dataframe
  - `def filter_xyz(df)`: Method to filter dataset based on some criteria

# Make Commands

```bash
rebuild:
	docker exec docker_jupyter bash -c "./scripts/rebuild_package.sh"

testing: rebuild
	docker exec docker_jupyter bash -c "pytest"

init:
	docker-compose -f docker-compose.yml up -d --build

up:
	docker-compose -f docker-compose.yml up -d

down:
	docker-compose -f docker-compose.yml down

build:
	docker-compose -f docker-compose.yml build
```
