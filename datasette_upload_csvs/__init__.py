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
        if await datasette.permission_allowed(actor, "upload-csvs", default=False):
            return [
                {"href": datasette.urls.path("/-/upload-csvs"), "label": "Upload CSVs"},
            ]

    return inner


async def upload_csvs(scope, receive, datasette, request):
    if not await datasette.permission_allowed(
        request.actor, "upload-csvs", default=False
    ):
        raise Forbidden("Permission denied for upload-csvs")

    # For the moment just use the first database that's not immutable
    db = [
        db
        for db in datasette.databases.values()
        if db.is_mutable and db.name != "_internal"
    ][0]

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
    filename = csv.filename
    if filename.endswith(".csv"):
        filename = filename[:-4]

    total_size = get_temporary_file_size(csv.file)
    task_id = str(uuid.uuid4())

    # Use the first 2MB to detect the character encoding
    first_bytes = csv.file.read(2048)
    csv.file.seek(0)
    encoding = detect(first_bytes)["encoding"]
    print(encoding)

    def insert_initial_record(conn):
        database = sqlite_utils.Database(conn)
        database["_csv_progress_"].insert(
            {
                "id": task_id,
                "filename": filename,
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

    def insert_docs(database):
        reader = csv_std.reader(codecs.iterdecode(csv.file, encoding))
        headers = next(reader)

        docs = (dict(zip(headers, row)) for row in reader)

        i = 0

        def docs_with_progress():
            nonlocal i
            for doc in docs:
                i += 1
                yield doc
                if i % 10 == 0:
                    database["_csv_progress_"].update(
                        task_id,
                        {
                            "rows_done": i,
                            "bytes_done": csv.file.tell(),
                        },
                    )

        database[filename].insert_all(docs_with_progress(), alter=True, batch_size=100)
        database["_csv_progress_"].update(
            task_id,
            {
                "rows_done": i,
                "bytes_done": total_size,
                "completed": str(datetime.datetime.utcnow()),
            },
        )
        return database[filename].count

    def insert_docs_catch_errors(conn):
        database = sqlite_utils.Database(conn)
        try:
            insert_docs(database)
        except Exception as error:
            database["_csv_progress_"].update(
                task_id,
                {"error": str(error)},
            )

    await db.execute_write_fn(insert_docs_catch_errors, block=False)

    if formdata.get("xhr"):
        return Response.json(
            {
                "url": datasette.urls.table(db.name, filename),
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
                "table": filename,
                "table_url": datasette.urls.table(db.name, filename),
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
