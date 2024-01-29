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
import threading
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
    db = dbs[0]

    # We need the ds_request to pass to render_template for CSRF tokens
    ds_request = request

    # We use the Starlette request object to handle file uploads
    starlette_request = Request(scope, receive)
    if starlette_request.method != "POST":
        return Response.html(
            await datasette.render_template(
                "upload_csv.html", {"database_name": db.name}, request=ds_request
            )
        )

    formdata = await starlette_request.form()
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

    # We run the CSV parser in a thread, sending 100 rows at a time to the DB
    def parse_csv_in_thread(event_loop, csv_file, db, table_name, task_id):
        try:
            reader = csv_std.reader(codecs.iterdecode(csv_file, encoding))
            headers = next(reader)

            tracker = TypeTracker()

            docs = tracker.wrap(dict(zip(headers, row)) for row in reader)

            i = 0

            def docs_with_progress():
                nonlocal i
                for doc in docs:
                    i += 1
                    yield doc
                    if i % 10 == 0:

                        def update_progress(conn):
                            database = sqlite_utils.Database(conn)
                            database["_csv_progress_"].update(
                                task_id,
                                {
                                    "rows_done": i,
                                    "bytes_done": csv_file.tell(),
                                },
                            )

                        future = asyncio.run_coroutine_threadsafe(
                            db.execute_write_fn(update_progress), event_loop
                        )
                        future.result()

            def write_batch(batch):
                def insert_batch(conn):
                    database = sqlite_utils.Database(conn)
                    database[table_name].insert_all(batch, alter=True)

                future = asyncio.run_coroutine_threadsafe(
                    db.execute_write_fn(insert_batch), event_loop
                )
                # Wait for it to finish so we don't overwhelm write queue
                future.result()

            batch = []
            batch_size = 0
            for doc in docs_with_progress():
                batch.append(doc)
                batch_size += 1
                if batch_size > 100:
                    write_batch(batch)
                    batch = []
                    batch_size = 0

            if batch:
                write_batch(batch)

            # Mark progress as complete
            def mark_complete(conn):
                nonlocal i
                database = sqlite_utils.Database(conn)
                database["_csv_progress_"].update(
                    task_id,
                    {
                        "rows_done": i,
                        "bytes_done": total_size,
                        "completed": str(datetime.datetime.utcnow()),
                    },
                )

            future = asyncio.run_coroutine_threadsafe(
                db.execute_write_fn(mark_complete), event_loop
            )
            future.result()

            # Transform columns to detected types
            def transform_columns(conn):
                database = sqlite_utils.Database(conn)
                database[table_name].transform(types=tracker.types)

            future = asyncio.run_coroutine_threadsafe(
                db.execute_write_fn(transform_columns), event_loop
            )
            future.result()
        except Exception as error:

            def insert_error(conn):
                database = sqlite_utils.Database(conn)
                database["_csv_progress_"].update(
                    task_id,
                    {"error": str(error)},
                )

            future = asyncio.run_coroutine_threadsafe(
                db.execute_write_fn(insert_error), event_loop
            )
            future.result()

    loop = asyncio.get_running_loop()

    # Start that thread running in the default executor in the background
    loop.run_in_executor(
        None, parse_csv_in_thread, loop, csv.file, db, table_name, task_id
    )

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
