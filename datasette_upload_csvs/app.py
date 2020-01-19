from starlette.responses import PlainTextResponse, HTMLResponse
from starlette.endpoints import HTTPEndpoint
import csv as csv_std
from html import escape
import codecs
import sqlite_utils


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
            """
        <h1>Upload a CSV</h1>
        <p>CSV will be imported into <strong>{}</strong></p>
        <form action="/-/upload-csv" method="post" enctype="multipart/form-data">
            <input type="file" name="csv"> <input type="submit" value="Upload">
        </form>
        """.format(
                escape(self.get_database().name)
            )
        )

    async def post(self, request):
        formdata = await request.form()
        csv = formdata["csv"]
        # csv.file is a SpooledTemporaryFile, I can read it directly
        # NOTE: this is blocking - a better implementation would run this
        # in a thread.
        filename = csv.filename
        # TODO: Support other encodings:
        reader = csv_std.reader(codecs.iterdecode(csv.file, "utf-8"))
        headers = next(reader)
        docs = (dict(zip(headers, row)) for row in reader)
        if filename.endswith(".csv"):
            filename = filename[:-4]
        # Import data into a table of that name using sqlite-utils
        db = self.get_database()
        # This didn't work because the DB connection was read-only:
        # def fn(conn):
        #     sqlite_utils.Database(conn)[filename].insert_all(docs, alter=True)
        # await db.execute_against_connection_in_thread(fn)

        # For the moment, total abuse of execute_against_connection_in_thread:
        def fn(conn):
            writable_conn = sqlite_utils.Database(db.path)
            writable_conn[filename].insert_all(docs, alter=True)

        await db.execute_against_connection_in_thread(fn)

        return PlainTextResponse(f"Uploaded! ")
