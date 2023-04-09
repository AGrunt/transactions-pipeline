# Transactions Pipeline Project

Transactions pipeline built with Apache Airflow and Great Expectations loads data from .csv file into data warehouse and validates data quality.

## Prerequsites

1. WSL 2.0
1. MySQL 8.0
1. Python 3.10.6
1. Airflow 2.5.0
1. Great Expectations 0.15.42
1. Configured Telegram Bot

## Configuration

### MySQL Schema Configuration

1. Connect to MySQL server instance 
2. Create `edw` schema
3. Execute .sql scripts `v1.1_create_table_transactions_landing.sql` and `v1.2_create_table_transactions.sql` to create tables in `edw` schema

### Airflow Providers

Airflow providers are installed as a part of virtual environment creation and Python package install.

```
pip install -r requirements.txt
```

### AirFlow Connections

Airflow requires "connection" objects to run Sensors and Operators such as MySQL Operator, Telegram Operator, File Sensor, etc.  

#### Raw Data MySQL Connection

1. Select *Admin/Connections* menu in AirFlow Web UI.
2. Press *Add new record (+)* button 
3. Fill in connection details:
   1. **Connection Id** - db_mysql_transactions_landing
   2. **Connection Type** - Choose "MySQL" type of connection
   3. **Description** - Landing area for raw transactions data exported from CBA online banking
   4. **Host** - IP, FQDN or hostname hosting MySQL service 
   5. **Scheme** - edw
   6. **Login** - Username that has access to MySQL service 
   7. **Password** - Password for Username
   8. **Port** - Host's port listening for MySQL connections
4. Press *Save* button

### MySQL Connection For Transformed Data

1. Select *Admin/Connections* menu in AirFlow Web UI.
2. Press *Add new record (+)* button 
3. Fill in connection details:
   1. **Connection Id** - db_mysql_transactions
   2. **Connection Type** - Choose "MySQL" type of connection
   3. **Description** - Cleaned up transactions data
   4. **Host** - IP, FQDN or hostname hosting MySQL service 
   5. **Scheme** - edw
   6. **Login** - Username that has access to MySQL service 
   7. **Password** - Password for Username
   8. **Port** - Host's port listening for MySQL connections
4. Press *Save* button

### File System connection

File system connection is used to access raw transaction files and need to be configured with a path to find them. To set up file system connection:

1. Select *Admin/Connections* menu in AirFlow Web UI.
2. Press "Add new record (+)" button
3. Fill in connection details:  
    1. **Connection Id** - file_csv_transactions_landing
    2. **Connection Type** - File (path)
    3. **Description** - Landing directory for transaction files.
    4. **Extra** - {"path": "/tmp/"}  
4. Press *Save* button


### Telegram Connection

Prior to setting up Telegram connection obtain your group chat ID

1. Go to `https://api.telegram.org/bot<API key for Telegramm bot>/getUpdates`
2. Note chat ID to use in the telegram connection creation steps below.

Telegram connection is used to send notification of DAG start, task success, and failure. To setup Telegram connection:
1. Select *Admin/Connections* menu in AirFlow Web UI.
2. Press "Add new record (+)" button
3. Fill in connection details:  
    1. **Connection Id** - telegram_default
    2. **Connection Type** - HTTP
    3. **Description** - telegram connection for messaging
    4. **Login** - Leave empty
    5. **Password** - API key for Telegramm bot
    6. **Extra** - {"chat_id": "`<Chat ID for telegram group chat>`"}
4. Press *Save* button

### Greate Expectations setup

Once prerequisites are installed, start Jupiter Notebook to create Great Expectations connection, expectation, and checkpoint.

```
jupyter notebook
```

Great Expectations is relying on environment variable substitution for database username and password configuration. Set `GE_USERNAME` and `GE_PASSWORD` environment variables accordingly before starting Airflow.

## How To Run

1. Run AirFlow in standalone mode: `airflow standalone`
2. Copy DAG file into Airflow DAG file location: `~/airflow/dags/`  
3. Drop raw transactions file obtain from CBA Online Banking into directory configured at the file system connection step in Airflow Connections section
4. Wait until the next hour for execution to be triggered automaticaly or navigate to Airflow Web UI to trigger execution manually  

## Design Decisions and Conventions

1. Follow service_source_action convention for naming variables, for example `mysql_datatable_hook`
2. Stick with Python naming conventions for functions and Airflow tasks