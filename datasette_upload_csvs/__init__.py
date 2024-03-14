import asyncio
from datasette import hookimpl
from datasette.utils.asgi import Response, Forbidden
from charset_normalizer import detect
from starlette.requests import Request
from urllib.parse import quote_plus
import csv as csv_std
import codecs
import datetime
import io
import os
import sqlite_utils
from sqlite_utils.utils import TypeTracker
import uuid


@hookimpl
def permission_allowed(actor, action):
    if action == "upload-csvs" and actor and actor.get("id") == "root":
        return True


@hookimpl
def register_routes():
    return [
        (r"^/-/upload-csvs$", upload_csvs),
        (r"^/-/upload-csv$", lambda: Response.redirect("/-/upload-csvs")),
    ]


@hookimpl
def menu_links(datasette, actor):
    async def inner():
        if await datasette.permission_allowed(
            actor, "upload-csvs", default=False
        ) and any(
            db.is_mutable and db.name not in ("_memory", "_internal")
            for db in datasette.databases.values()
        ):
            return [
                {"href": datasette.urls.path("/-/upload-csvs"), "label": "Upload CSVs"},
            ]

    return inner


@hookimpl
def database_actions(datasette, actor, database):
    async def inner():
        db = datasette.get_database(database)
        if (
            await datasette.permission_allowed(actor, "upload-csvs", default=False)
            and db.is_mutable
            and db.name not in ("_memory", "_internal")
        ):
            return [
                {
                    "href": datasette.urls.path(
                        "/-/upload-csvs?database={}".format(quote_plus(db.name))
                    ),
                    "label": "Upload CSV",
                    "description": "Create a new table by uploading a CSV file",
                }
            ]

    return inner


async def upload_csvs(scope, receive, datasette, request):
    if not await datasette.permission_allowed(
        request.actor, "upload-csvs", default=False
    ):
        raise Forbidden("Permission denied for upload-csvs")

    num_bytes_to_detect_with = 2048 * 1024
    # ?_num_bytes= can over-ride this, used by the tests
    if request.args.get("_num_bytes_to_detect_with"):
        num_bytes_to_detect_with = int(request.args["_num_bytes_to_detect_with"])

    # For the moment just use the first database that's not immutable
    dbs = [
        db
        for db in datasette.databases.values()
        if db.is_mutable and db.name not in ("_internal", "_memory")
    ]
    if not dbs:
        raise Forbidden("No mutable databases available")

    default_db = dbs[0]

    # We need the ds_request to pass to render_template for CSRF tokens
    ds_request = request

    # We use the Starlette request object to handle file uploads
    starlette_request = Request(scope, receive)
    if starlette_request.method != "POST":
        selected_db = ds_request.args.get("database")
        databases = []
        # If there are multiple databases let them choose
        if len(dbs) > 1:
            databases = [
                {"name": db.name, "selected": db.name == selected_db} for db in dbs
            ]
        return Response.html(
            await datasette.render_template(
                "upload_csv.html",
                {"databases": databases, "selected_name": selected_db},
                request=ds_request,
            )
        )

    formdata = await starlette_request.form()
    database_name = formdata.get("database") or default_db.name
    db = datasette.get_database(database_name)
    csv = formdata["csv"]
    # csv.file is a SpooledTemporaryFile. csv.filename is the filename
    table_name = formdata.get("table")
    if not table_name:
        table_name = csv.filename
        if table_name.endswith(".csv"):
            table_name = table_name[:-4]

    # If the table already exists, add a suffix
    suffix = 2
    base_table_name = table_name
    while await db.table_exists(table_name):
        table_name = "{}_{}".format(base_table_name, suffix)
        suffix += 1

    total_size = get_temporary_file_size(csv.file)
    task_id = str(uuid.uuid4())

    # Use the first 2MB to detect the character encoding
    first_bytes = csv.file.read(num_bytes_to_detect_with)
    csv.file.seek(0)
    encoding = detect(first_bytes)["encoding"]

    # latin-1 is a superset of ascii, and less likely to hit errors
    # https://github.com/simonw/datasette-upload-csvs/issues/25
    if encoding == "ascii":
        encoding = "latin-1"

    def insert_initial_record(conn):
        database = sqlite_utils.Database(conn)
        with conn:
            database["_csv_progress_"].insert(
                {
                    "id": task_id,
                    "table_name": table_name,
                    "bytes_todo": total_size,
                    "bytes_done": 0,
                    "rows_done": 0,
                    "started": str(datetime.datetime.utcnow()),
                    "completed": None,
                    "error": None,
                },
                pk="id",
                alter=True,
            )

    await db.execute_write_fn(insert_initial_record)

    def make_insert_batch(batch):
        def inner(conn):
            db = sqlite_utils.Database(conn)
            with conn:
                db[table_name].insert_all(batch, alter=True)

        return inner

    # We run a parser in a separate async task, writing and yielding every 100 rows
    async def parse_csv():
        i = 0
        tracker = TypeTracker()
        try:
            reader = csv_std.reader(codecs.iterdecode(csv.file, encoding))
            headers = next(reader)

            docs = tracker.wrap(dict(zip(headers, row)) for row in reader)

            batch = []
            for doc in docs:
                batch.append(doc)
                i += 1
                if i % 10 == 0:
                    await db.execute_write(
                        "update _csv_progress_ set rows_done = ?, bytes_done = ? where id = ?",
                        (i, csv.file.tell(), task_id),
                    )
                if i % 100 == 0:
                    await db.execute_write_fn(make_insert_batch(batch))
                    batch = []
                    # And yield to the event loop
                    await asyncio.sleep(0)

            if batch:
                await db.execute_write_fn(make_insert_batch(batch))

            # Mark progress as complete
            def mark_complete(conn):
                nonlocal i
                database = sqlite_utils.Database(conn)
                with conn:
                    database["_csv_progress_"].update(
                        task_id,
                        {
                            "rows_done": i,
                            "bytes_done": total_size,
                            "completed": str(datetime.datetime.utcnow()),
                        },
                    )

            await db.execute_write_fn(mark_complete)

            # Transform columns to detected types
            def transform_columns(conn):
                database = sqlite_utils.Database(conn)
                with conn:
                    database[table_name].transform(types=tracker.types)

            await db.execute_write_fn(transform_columns)

        except Exception as error:
            await db.execute_write(
                "update _csv_progress_ set error = ? where id = ?",
                (str(error), task_id),
            )

    # Run that as a task
    asyncio.create_task(parse_csv())

    if formdata.get("xhr"):
        return Response.json(
            {
                "url": datasette.urls.table(db.name, table_name),
                "database_path": quote_plus(db.name),
                "task_id": task_id,
                "bytes_todo": total_size,
            }
        )

    return Response.html(
        await datasette.render_template(
            "upload_csv_done.html",
            {
                "database": db.name,
                "table": table_name,
                "table_url": datasette.urls.table(db.name, table_name),
            },
        )
    )


def get_temporary_file_size(file):
    if isinstance(file._file, (io.BytesIO, io.StringIO)):
        return len(file._file.getvalue())
    try:
        return os.fstat(file._file.fileno()).st_size
    except Exception:
        raise
