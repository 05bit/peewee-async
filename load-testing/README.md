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

Firewall rulle to make postgreql connection unreacheable

```bash
sudo iptables -I INPUT -p tcp --dport 5432 -j DROP
```

Revert the rule back:


```bash
sudo iptables -D INPUT 1
```
