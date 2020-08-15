from datasette.app import Datasette
import asyncio
from asgi_lifespan import LifespanManager
import json
import pytest
import httpx
import sqlite_utils


@pytest.mark.asyncio
async def test_lifespan():
    ds = Datasette([], memory=True)
    app = ds.app()
    async with LifespanManager(app):
        async with httpx.AsyncClient(app=app) as client:
            response = await client.get("http://localhost/")
            assert 200 == response.status_code


@pytest.mark.asyncio
async def test_redirect():
    datasette = Datasette([], memory=True)
    # First test the upload page exists
    async with httpx.AsyncClient(app=datasette.app()) as client:
        response = await client.get(
            "http://localhost/-/upload-csv", allow_redirects=False
        )
        assert response.status_code == 302
        assert response.headers["location"] == "/-/upload-csvs"


@pytest.mark.asyncio
async def test_upload(tmpdir):
    path = str(tmpdir / "data.db")
    db = sqlite_utils.Database(path)

    db["hello"].insert({"hello": "world"})

    datasette = Datasette([path])

    # First test the upload page exists
    async with httpx.AsyncClient(app=datasette.app()) as client:
        response = await client.get("http://localhost/-/upload-csv")
        assert 200 == response.status_code
        assert b'<form action="/-/upload-csv" method="post"' in response.content
        csrftoken = response.cookies["ds_csrftoken"]

        # Now try uploading a file
        files = {"csv": ("dogs.csv", "name,age\nCleo,5\nPancakes,4", "text/csv")}
        response = await client.post(
            "http://localhost/-/upload-csv", data={"csrftoken": csrftoken}, files=files
        )
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
