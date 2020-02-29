from starlette.responses import PlainTextResponse, HTMLResponse
from starlette.endpoints import HTTPEndpoint
import csv as csv_std
import codecs
import io
import os
import sqlite_utils
import uuid


class UploadApp(HTTPEndpoint):
    def __init__(self, scope, receive, send, datasette):
        self.datasette = datasette
        super().__init__(scope, receive, send)

    def get_database(self):
        # For the moment just use the first one that's not immutable
        mutable = [db for db in self.datasette.databases.values() if db.is_mutable]
        return mutable[0]

    async def get(self, request):
        return HTMLResponse(
            await self.datasette.render_template(
                "upload_csv.html", {"database_name": self.get_database().name}
            )
        )

    async def post(self, request):
        formdata = await request.form()
        csv = formdata["csv"]
        # csv.file is a SpooledTemporaryFile. csv.filename is the filename
        filename = csv.filename
        if filename.endswith(".csv"):
            filename = filename[:-4]

        # Import data into a table of that name using sqlite-utils
        db = self.get_database()

        total_size = get_temporary_file_size(csv.file)

        def fn(conn):

            # TODO: Support other encodings:
            reader = csv_std.reader(codecs.iterdecode(csv.file, "utf-8"))
            headers = next(reader)

            docs = (dict(zip(headers, row)) for row in reader)

            database = sqlite_utils.Database(conn)
            task_id = str(uuid.uuid4())
            database["_csv_progress_"].insert(
                {
                    "id": task_id,
                    "filename": filename,
                    "todo": total_size,
                    "done": 0,
                    "rows": 0,
                },
                pk="id",
            )

            def docs_with_progress():
                i = 0
                for doc in docs:
                    i += 1
                    yield doc
                    if i % 10 == 0:
                        database["_csv_progress_"].update(
                            task_id, {"rows": i, "done": csv.file.tell(),}
                        )

            database[filename].insert_all(
                docs_with_progress(), alter=True, batch_size=100
            )
            return database[filename].count

        await db.execute_write_fn(fn)

        return HTMLResponse(
            await self.datasette.render_template(
                "upload_csv_done.html",
                {
                    "database": self.get_database().name,
                    "table": filename,
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
