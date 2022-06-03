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
    async with httpx.AsyncClient(app=datasette.app()) as client:
        response = await client.get("http://localhost/-/upload-csv")
        assert response.status_code == 302
        assert response.headers["location"] == "/-/upload-csvs"


@pytest.mark.asyncio
@pytest.mark.parametrize("auth", [True, False])
async def test_menu(auth):
    ds = Datasette([], memory=True)
    app = ds.app()
    async with LifespanManager(app):
        async with httpx.AsyncClient(app=app) as client:
            cookies = {}
            if auth:
                cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
            response = await client.get("http://localhost/", cookies=cookies)
            assert 200 == response.status_code
            if auth:
                assert "/-/upload-csvs" in response.text
            else:
                assert "/-/upload-csvs" not in response.text


@pytest.mark.asyncio
async def test_upload(tmpdir):
    path = str(tmpdir / "data.db")
    db = sqlite_utils.Database(path)

    db["hello"].insert({"hello": "world"})

    datasette = Datasette([path])

    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}

    # First test the upload page exists
    async with httpx.AsyncClient(app=datasette.app()) as client:
        response = await client.get("http://localhost/-/upload-csvs", cookies=cookies)
        assert 200 == response.status_code
        assert b'<form action="/-/upload-csvs" method="post"' in response.content
        csrftoken = response.cookies["ds_csrftoken"]
        cookies["ds_csrftoken"] = csrftoken

        # Now try uploading a file
        files = {"csv": ("dogs.csv", b"name,age\nCleo,5\nPancakes,4", "text/csv")}
        response = await client.post(
            "http://localhost/-/upload-csvs",
            cookies=cookies,
            data={"csrftoken": csrftoken},
            files=files,
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


@pytest.mark.asyncio
async def test_permissions(tmpdir):
    path = str(tmpdir / "data.db")
    db = sqlite_utils.Database(path)["foo"].insert({"hello": "world"})
    ds = Datasette([path])
    app = ds.app()
    async with httpx.AsyncClient(app=app) as client:
        response = await client.get("http://localhost/-/upload-csvs")
        assert 403 == response.status_code
    # Now try with a root actor
    async with httpx.AsyncClient(app=app) as client2:
        response2 = await client2.get(
            "http://localhost/-/upload-csvs",
            cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
        )
        assert 403 != response2.status_code
