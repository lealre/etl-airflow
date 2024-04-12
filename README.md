# Orchestrating an ETL with Airflow - From Google Drive to PostgreSQL

This project aims to orchestrate an ETL (Extract-Transform-Load) with Airflow, extracting CSV files from a folder in Google Drive, transforming values, and storing them in a PostgreSQL database.

The data is handled in a pandas DataFrame format, and all the data validation is performed using the [Pandera](https://pandera.readthedocs.io/en/stable/#) library, a Pydantic-based library to validate DataFrame schemas. By setting a specific data contract, validations occur in two phases: when extracted and when transformed.

The Airflow implementation was created using the Astro CLI, the command line interface for data orchestration from [Astronomer](https://docs.astronomer.io/).

This project also has a CI for every Pull Request made using GitHub Actions, where the schema contract is tested with the pytest library.

![](pics/etl-diagram.png)

## Context

This project was built in a context where a folder inside Google Drive regularly receives different CSV files containing the operational revenue from various companies in three types of currencies: USD, EUR, and YEN. This data must be stored in a PostgreSQL database with the operational revenue converted to USD and the currency conversion rate, taking the last quotation of the month date that is in the CSV.

### Contract Schema

The project uses the following contract schema to validate the data:

* Schema-in: Used when extracting files from Google Drive in Task 02

| Column               | Type                        | Constraints                                    |
|----------------------|-----------------------------|------------------------------------------------|
| company              | Series[str]                 |                                                |
| currency             | Series[str]                 | in ['EUR', 'USD', 'YEN'], all values equal                     |
| operational_revenue  | Series[float]               | greater than or equal to 0                    |
| date                 | Series[pa.DateTime]         | all values equal                                               |
| file_id              | Optional[str]               |                                                |

* Schema-out: Used when transforming data in Task 03

| Column            | Type                | Constraints                     |
|-------------------|---------------------|---------------------------------|
| company           | Series[str]         |                                 |
| currency          | Series[str]         | in ['EUR', 'USD', 'YEN'], all values equal      |
| operational_revenue | Series[float]    | greater than or equal to 0     |
| date              | Series[pa.DateTime] | all values equal                             |
| file_id           | Series[str]         |                                 |
| convertion_rate   | Series[float]       | greater than or equal to 0     |
| usd_converted     | Series[float]       | greater than or equal to 0     |

## How it works

**<u>Task 01:</u> Connect with Google Drive API and extract CSV files**

In this initial task, the script connects to the Google Drive API by passing our credentials in JSON file format and specifying the parent folder name and the folder from which we want to extract the CSV files. Subsequently, it retrieves all file information from the designated folder, including file name, Google Drive file ID, and file type.

Google Drive assigns a unique ID to each file uploaded to its folders, which is inserted into the database as a unique file identifier. This allows for filtering to determine if the file has already been uploaded to the database in subsequent Airflow triggers.

To facilitate this task, a GoogleDrive class was created to encapsulate all desired functionalities of the Google Drive API in `google_drive.py`. 

The task output is a list of DataFrames, where each DataFrame represents a CSV file extracted from the Google Drive folder.

**<u>Task 02:</u> Validate extracted data**

Receives the list of DataFrames extracted from Task 01 and validates it according to the contract schema using the Pandera library, raising an invalid schema error if applicable.

The output is a list of validated DataFrames.

**<u>Task 03:</u> Transform data and validate it**

Receives a list of DataFrames and performs the transformations. It maps both the currency and the date from each DataFrame and includes the currency rate conversion and the USD amount converted columns using the rate conversion from that date. It also performs schema validation after this transformation using Pandera.

The conversion rate data is obtained from the FRED `pandas_datareader` library, from the links below:

* [U.S. Dollars to Euro Spot Exchange Rate](https://fred.stlouisfed.org/series/DEXUSEU)
* [Japanese Yen to U.S. Dollar Spot Exchange Rate](https://fred.stlouisfed.org/series/DEXJPUS)

The output is a list of transformed and validated DataFrames.

**<u>Task 04:</u> Load data in database**

This final task loads the data into a PostgreSQL database. 

### Project Folder Structure

```
├── Dockerfile
├── README.md
├── airflow_settings.yaml
├── dags
│   └── dag_etl.py
├── dev-requirements.txt
├── packages.txt
├── pics
│   └── etl-diagram.png
├── pyproject.toml
├── pytest.ini
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── database.py
│   ├── etl.py
│   ├── google_drive.py
│   ├── main.py
│   ├── schema.py
│   └── transform_utils.py
└── tests
    ├── dags
    │   └── test_dag_example.py
    ├── test_schema_in.py
    └── test_schema_out.py
```


## How to run this project

