peewee-async load testing
============

Created for help to find race conditions in the library

How to use
-------

Install requirements:

```bash
pip install requirments
```

Run the app:

```bash
uvicorn app:app
```

Run yandex-tank:

```bash
docker run -v $(pwd):/var/loadtest --net host -it yandex/yandex-tank
```
