# Google Cloud Storage(GCS) Data Ingestion and Building Data Lake Example 🚀

This is a personel project 🚀 to try pythonic code standarts and pyspark 🐍

## Aim of the Project 🎯

There are some kind of e-commerce data stored on google cloud storage as json format. 
I am aiming to achieve these items within the project:

-   Ingesting data from GCS with Pyspark 
-   To be able to make intended transformations on Pyspark
-   Desinging a Data Lake adopting Medallian Architecture
-   Adopting snapshotting of data to be able to track new addendum and being able to revert previous version
-   Implementing code quality standarts like ci/cd and testing

## How to run? 🏃‍♀️

Please follow these steps:

**Running with Docker**

1. Clone this project to your local environment
2. Run `docker-compose build` on the terminal. Please be sure thayou should be in the same directory with docker-compose yaml.
3. Run `docker-compose up` on terminal again. 
4. Then, you can connect to swagger document of application vihttp://0.0.0.0:8000/docs

## Google Cloud Storage Structure ☁️

The source files are stored in a hierarchy given below:

```
+-- Buckets
|   +-- webshop-simulation-streaming-landing
|   |   +-- prod
|   |   |   +-- webshop.public.category/
|   |   |   |   +-- file1.json
|   |   |   |   +-- file2.json
|   |   |   |   +-- ...
|   |   |   +-- webshop.public.customer/
|   |   |   |   +-- file1.json
|   |   |   |   +-- file2.json
|   |   |   |   +-- ...
|   |   |   +-- webshop.public.customeradress/
...
|   |   |   +-- webshop.public.customerpaymentprovider/
|   |   |   +-- webshop.public.event/
|   |   |   +-- webshop.public.orders/
|   |   |   +-- webshop.public.paymentprovider/
|   |   |   +-- webshop.public.product/
|   |   |   +-- webshop.public.productbrand/
|   |   |   +-- webshop.public.productcategory/
```

Data will be divided 3 layers inside data lake from raw to the most mature version, and stored by snapshotting. I assumed that storage cost is not  much and being able to return a previous version is really essential for this imaginary multi-billion company. 

The sink or data lake is stored in a structure like this:

```
+-- Buckets
|   +-- webshop-simulation-streaming-landing
|   |   +-- <name-of-datalake> (I used my name :d)
|   |   |   +-- bronze/silver/gold (a folder for each layer)
|   |   |   |   +-- webshop.public.category/
|   |   |   |   |   +-- <processTime>
|   |   |   |   |   |   +-- file1.json
|   |   |   |   |   |   +-- file2.json
|   |   |   |   |   |   +-- ...
|   |   |   |   +-- webshop.public.customer/
|   |   |   |   |   +-- <processTime>
|   |   |   |   |   |   +-- file1.json
|   |   |   |   |   |   +-- file2.json
|   |   |   |   |   |   +-- ...
...
|   |   |   |   +-- webshop.public.customerpaymentprovider/
|   |   |   |   +-- webshop.public.event/
|   |   |   |   +-- webshop.public.orders/
|   |   |   |   +-- webshop.public.paymentprovider/
|   |   |   |   +-- webshop.public.product/
|   |   |   |   +-- webshop.public.productbrand/
|   |   |   |   +-- webshop.public.productcategory/
```

## Data Lake Design 🌊

I followed the [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) for designing data lake implementation. 

I will have 3 layers when it is completely done.
    
1. **Bronze:** Raw data are directly read from source and kept on that layer without any transformation. This will give us the ability to track the differences between source system and object storage when there is a problem in prod. Metadata information is also kept here.

2. **Silver:** Basic transformations like millis to human-readable time format change and aliasing for columns are made on that layer. Besides, metadata information is excluded for that layer.

3. **Gold:**  I didn't start to build that layer but my plans are like adopting star schema modelling to create 2-3 big summary tables. These tables will be ready for directly being used in BI tools and analytical purposes. Important thing is; this layer is really dependent on the requirements and needs of businesses inside corporations.




