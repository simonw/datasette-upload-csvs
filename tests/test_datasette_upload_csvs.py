from datasette.app import Datasette
import asyncio
import json
import pytest
import httpx
import sqlite_utils


@pytest.mark.asyncio
async def test_upload(tmpdir):
    path = str(tmpdir / "data.db")
    db = sqlite_utils.Database(path)

    db["hello"].insert({"hello": "world"})

    datasette = Datasette([path])

    # First test the upload page exists

    dispatch = httpx.ASGIDispatch(
        app=datasette.app(), client=("1.2.3.4", 123), raise_app_exceptions=True
    )

    async with httpx.AsyncClient(dispatch=dispatch) as client:
        response = await client.get("http://localhost/-/upload-csv")
        assert 200 == response.status_code
        assert b'<form action="/-/upload-csv" method="post"' in response.content

        # Now try uploading a file
        files = {"csv": ("dogs.csv", "name,age\nCleo,5\nPancakes,4", "text/csv")}
        response = await client.post("http://localhost/-/upload-csv", files=files)
        assert b"<h1>Upload in progress</h1>" in response.content

        # Now things get tricky... the upload is running in a thread, so poll for completion
        await asyncio.sleep(1)
        response = await client.get(
            "http://localhost/data/_csv_progress_.json?_shape=array"
        )
        rows = json.loads(response.content)
        assert 1 == len(rows)
        assert {
            "filename": "dogs",
            "bytes_todo": 26,
            "bytes_done": 26,
            "rows_done": 2,
        }.items() <= rows[0].items()

    dogs = list(db["dogs"].rows)
    assert [{"name": "Cleo", "age": "5"}, {"name": "Pancakes", "age": "4"}] == dogs
