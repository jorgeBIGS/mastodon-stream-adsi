# Mastodon usage

Project obtained from the article "https://betterprogramming.pub/mastodon-usage-counting-toots-with-kafka-duckdb-seaborn-42215c9488ac" and adapted for scholar purposes by Jorge Garcia.

[Mastodon](https://joinmastodon.org/) is a _decentralized_ social networking platform. Mastodon users are members of a _specific_ Mastodon server, and servers are capable of joining other servers to form a global (or at least federated) social network.

Tools used
- [Mastodon.py](https://mastodonpy.readthedocs.io/) - Python library for interacting with the Mastodon API
- [Apache Kafka](https://kafka.apache.org/) - distributed event streaming platform
- [DuckDB](https://duckdb.org/) - in-process SQL OLAP database and the [HTTPFS DuckDB extension](https://duckdb.org/docs/extensions/httpfs.html) for reading remote/writing remote files of object storage using the S3 API
- [MinIO](https://min.io/) - S3 compatible server

# Data processing
We will use Kafka as distributed stream processing platform to collect data from multiple instances. To run Kafka, Kafka Connect (with the S3 sink connector) and schema registry (to support AVRO serialisation) and MinIO setup containers with this command

```console
 config/docker/docker-compose up -d
 ```

# Data collection

## Setup virtual python environment
Create a [virtual python](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/) environment to keep dependencies separate. The _venv_ module is the preferred way to create and manage virtual environments. 

 ```console
python3 -m venv env
```

Before you can start installing or using packages in your virtual environment you’ll need to activate it.

```console
source env/bin/activate
pip install --upgrade pip
pip install -r config/requirements.txt
 ```


## Federated timeline
These are the most recent public posts from people on this and other servers of the decentralized network that this server knows about.

## Mastodon listener
The python `mastodonlisten` application listens for public posts to the specified server, and sends each toot to Kafka. You can run multiple Mastodon listeners, each listening to the activity of different servers

```console
python src/mastodon_stream.py --baseURL https://mastodon.social --enableKafka
```

## Testing producer (optional)
As an optional step, you can check that AVRO messages are being written to kafka

```console
python src/kafka/kafka_consumer.py
```


# Kafka Connect
To load the Kafka Connect [config](./config/docker/minio/mastodon-sink-s3-minio.json) file run the following

```console
curl -X PUT -H  "Content-Type:application/json" localhost:8083/connectors/mastodon-sink-s3/config -d '@./config/docker/minio/mastodon-sink-s3-minio.json'
```

# Open s3 browser
Go to the MinIO web browser http://localhost:9001/

- username `minio`
- password `minio123`



## Query parquet files directly from s3 using DuckDB

Load the [HTTPFS DuckDB extension](https://duckdb.org/docs/extensions/httpfs.html) for reading remote/writing remote files of object storage using the S3 API

```console
INSTALL httpfs;
LOAD httpfs;

set s3_endpoint='localhost:9000';
set s3_access_key_id='minio';
set s3_secret_access_key='minio123';
set s3_use_ssl=false;
set s3_url_style='path';
```

And you can now query the parquet files directly from s3

```sql
select *
from read_parquet('s3://mastodon/topics/mastodon-topic/partition=0/*');
```

# Optional steps


## Cleanup of virtual environment
If you want to switch projects or otherwise leave your virtual environment, simply run:

```console
deactivate
```

If you want to re-enter the virtual environment just follow the same instructions above about activating a virtual environment. There’s no need to re-create the virtual environment.

