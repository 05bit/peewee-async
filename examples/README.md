Examples for peewee-async
=========================

To run the examples install dependencies first:

```bash
pip install -r examples/requirements.txt
```

Also please run database service and provide credentials in environment variables.
Feel free to use development database services with the default credentials, e.g:

```bash
docker compose up postgres
```

## Example for `aiohttp` server


Define database connection settings if needed, environment variables used:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_HOST`
- `POSTGRES_PORT`

Run this command to create example tables in the database:

```bash
python -m examples.aiohttp_example
```

Run this command to start an example application:

```bash
gunicorn --bind 127.0.0.1:8080 --log-level INFO --access-logfile - \
      --worker-class aiohttp.GunicornWebWorker --reload \
      examples.aiohttp_example:app
```

Application should be up and running:

```bash
curl 'http://127.0.0.1:8080/?p=1'
```

the output should be:

```
This is a first post
```
