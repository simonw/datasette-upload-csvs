from datasette.app import Datasette
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
    async with httpx.AsyncClient(app=datasette.app()) as client:
        response = await client.get("http://localhost/-/upload-csv")
        assert 200 == response.status_code
        assert b'<form action="/-/upload-csv" method="post"' in response.content

        # Now try uploading a file
        files = {"csv": ("dogs.csv", "name,age\nCleo,5\nPancakes,4", "text/csv")}
        response = await client.post("http://localhost/-/upload-csv", files=files)
        assert b"<h1>Upload complete</h1>" in response.content
        assert b"Imported 2 rows into" in response.content

    dogs = list(db["dogs"].rows)
    assert [{"name": "Cleo", "age": "5"}, {"name": "Pancakes", "age": "4"}] == dogs
