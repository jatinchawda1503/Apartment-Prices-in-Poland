# Apartment-Prices-in-Poland

## Setup

### 1. Setup Enviornment

```
conda create -n apip python=3.11
conda activate apip
```

### 2. Install the dependency libararies
```
pip install -r requirements.txt
```

### 3. Install postgres 

1. Install and start the server
```
sudo apt update
sudo apt install postgresql postgresql-contrib libpq-dev

psql --version

sudo service postgresql status

sudo service postgresql start

sudo passwd postgres
postgres
postgres
```

2. Setup config connection to connect with PgAdmin

```
 sudo -u postgres psql -c 'SHOW config_file'
               config_file

sudo nano /etc/postgresql/15/main/postgresql.conf
```

3. Change listen_addresses variable to *
```
listen_addresses = '*'
```

4. Add additonal configration to pga_hba.conf

```
sudo nano /etc/postgresql/15/main/pg_hba.conf

host    all             all             0.0.0.0/0            md5
host    all             all             ::/0                 md5
```

5. Restart the server 

```
sudo service postgresql restart
```

5. Get the IP address of the System

```
hostname -I

```

6. Connect with the pgAdmin
    i. In localhost, right-click Servers ->Create->Server to create a new server
    ii. General tab - set your name e.g. - WSL 
    iii. Connection tab -  
        Host - your ip from hostname
        Post - 5432
        username - postgres
        password - your_pass

### 4. Setup the Airflow Server

1. Export the AIRFLOW_PATH 

```
sudo nano ~/.bashrc 
export AIRFLOW_HOME=/mnt/d/Path/To/Project/Folder
```
2. Setup the Airflow Database in the project directory

```
cd $AIRFLOW_HOME
airflow db init
```
*** optional: setup database using [Postgres](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-postgresql-database)

3. Create User 

```
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```

4. Create .env file

```
DB_NAME = "apip"
DB_USER = 'postgres'
DB_PASSWORD = "postgres"
DB_HOST = "172.20.44.139"
```
5. Run Airflow Webserver

```
airflow webserver
```

6. Run Airflow Scheduler
```
airflow scheduler
```





 
