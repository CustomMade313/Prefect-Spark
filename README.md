# Prefect-Spark
A simple example of Prefect integration with Spark for educational purposes.

## Table of Contents

- [About](#about)
- [Getting Started](#getting-started)
- [Prerequisites](#rerequisites)

## About
Just a simple example of pyspark and prefect integration. This project uses a [Netflix Dataset](https://www.kaggle.com/datasets/victorsoeiro/netflix-tv-shows-and-movies). 
This project performs the following tasks:
1. Drops some columns from the titles file.
2. Converts the CSV format of the files to Parquet.
3. Executes a join query on the two files.


## Getting Started

To run this project, you'll need to have the following dependencies installed:

- [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html)
- [Prefect](https://docs.prefect.io/)
- [pandas](https://pandas.pydata.org/)

### Prerequisites

Make sure you have Python and pip installed. Then, install the required

```bash
# Example
pip install pyspark
pip install prefect
pip isntall pandas
