import json
import logging
import os
from secrets import token_hex
from datetime import datetime
from aiohttp import web
from peewee import CharField, TextField, DateTimeField
from peewee_async import PooledPostgresqlDatabase, AioModel

logger = logging.getLogger(__name__)

database = PooledPostgresqlDatabase(
    os.environ.get('POSTGRES_DB', 'postgres'),
    user=os.environ.get('POSTGRES_USER', 'postgres'),
    password=os.environ.get('POSTGRES_PASSWORD', 'postgres'),
    host=os.environ.get('POSTGRES_HOST', '127.0.0.1'),
    port=int(os.environ.get('POSTGRES_PORT', 5432)),
    min_connections=2,
    max_connections=10,
)

app = web.Application()

routes = web.RouteTableDef()


class Post(AioModel):
    title = CharField(unique=True)
    key = CharField(unique=True, default=lambda: token_hex(8))
    text = TextField()
    created_at = DateTimeField(index=True, default=datetime.utcnow)

    class Meta:
        database = database

    def __str__(self):
        return self.title


def add_post(title, text):
    with database.atomic():
        Post.create(title=title, text=text)


@routes.get('/')
async def get_post_endpoint(request):
    query = dict(request.query)
    post_id = query.pop('p', 1)
    post = await Post.aio_get_or_none(id=post_id)
    if post:
        return web.Response(text=post.text)
    else:
        return web.Response(text="Not found", status=404)


@routes.post('/')
async def update_post_endpoint(request):
    query = dict(request.query)
    post_id = query.pop('p', 1)
    try:
        data = await request.content.read()
        data = json.loads(data)
        text = data.get('text')
        if not text:
            raise ValueError("Missing 'text' in data")
    except Exception as exc:
        return web.Response(text=str(exc), status=400)

    post = await Post.aio_get_or_none(id=post_id)
    if post:
        post.text = text
        await post.aio_save()
        return web.Response(text=post.text)
    else:
        return web.Response(text="Not found", status=404)


# Setup application routes

app.add_routes(routes)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    print("Initialize tables and add some random posts...")

    try:
        with database:
            database.create_tables([Post], safe=True)
            print("Tables are created.")
    except Exception as exc:
        print("Error creating tables: {}".format(exc))

    try:
        add_post("Hello, world", "This is a first post")
        add_post("Hello, world 2", "This is a second post")
        add_post("42", "What is this all about?")
        add_post("Let it be!", "Let it be, let it be, let it be, let it be")
        print("Done.")
    except Exception as exc:
        print("Error adding posts: {}".format(exc))
