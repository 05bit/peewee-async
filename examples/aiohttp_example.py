import json
import logging
import os
# from asyncio import sleep as async_sleep
from secrets import token_hex
from datetime import datetime
from aiohttp import web
from peewee import Model, CharField, TextField, DateTimeField
from peewee_async import PooledPostgresqlDatabase, Manager

logger = logging.getLogger(__name__)

database = PooledPostgresqlDatabase(
    'examples', user='postgres', password='postgres',
    host='db-postgres', port=5432,
    min_connections=10, max_connections=100,
)

objects = Manager(database)

app = web.Application()

routes = web.RouteTableDef()


class Post(Model):
    title = CharField(unique=True)
    key = CharField(unique=True, default=lambda: token_hex(8))
    text = TextField()
    created_at = DateTimeField(index=True, default=datetime.utcnow)

    class Meta:
        database = database

    def __str__(self):
        return self.title


def create_tables():
    with database:
        database.create_tables([Post], safe=True)


def add_post(title, text):
    with database.atomic():
        Post.create(title=title, text=text)


@routes.get('/')
async def get_post_endpoint(request):
    query = dict(request.query)
    post_id = query.pop('p', 1)
    post = await objects.get_or_none(Post, id=post_id)
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

    post = await objects.get_or_none(Post, id=post_id)
    if post:
        post.text = text
        await objects.update(post)
        return web.Response(text=post.text)
    else:
        return web.Response(text="Not found", status=404)


# Setup application routes

app.add_routes(routes)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # Initialize database
    create_tables()

    # Add some random posts
    try:
        add_post("Hello, world", "This is a first post")
        add_post("Hello, world 2", "This is a second post")
        add_post("42", "What is this all about?")
        add_post("Let it be!", "Let it be, let it be, let it be, let it be")
    except Exception as e:
        print("Error adding posts: {}".format(e))

    # Run application server
    port = os.environ.get('HTTP_PORT', 10080)
    app.run(port=port, host='0.0.0.0')
