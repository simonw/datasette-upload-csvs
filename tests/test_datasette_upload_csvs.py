from datasette.app import Datasette
import asyncio
from asgi_lifespan import LifespanManager
import json
from unittest.mock import ANY
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


SIMPLE = b"name,age\nCleo,5\nPancakes,4"
SIMPLE_EXPECTED = [{"name": "Cleo", "age": 5}, {"name": "Pancakes", "age": 4}]
NOT_UTF8 = (
    b"IncidentNumber,DateTimeOfCall,CalYear,FinYear,TypeOfIncident,PumpCount,PumpHoursTotal,HourlyNotionalCost(\xa3),IncidentNotionalCost(\xa3)\r\n"
    b"139091,01/01/2009 03:01,2009,2008/09,Special Service,1,2,2.55,5.10\r\n"
    b"275091,01/01/2009 08:51,2009,2008/09,Special Service,1,1,2.55,2.55"
)
NOT_UTF8_EXPECTED = [
    {
        "IncidentNumber": 139091,
        "DateTimeOfCall": "01/01/2009 03:01",
        "CalYear": 2009,
        "FinYear": "2008/09",
        "TypeOfIncident": "Special Service",
        "PumpCount": 1,
        "PumpHoursTotal": 2,
        "HourlyNotionalCost(£)": 2.55,
        "IncidentNotionalCost(£)": 5.10,
    },
    {
        "IncidentNumber": 275091,
        "DateTimeOfCall": "01/01/2009 08:51",
        "CalYear": 2009,
        "FinYear": "2008/09",
        "TypeOfIncident": "Special Service",
        "PumpCount": 1,
        "PumpHoursTotal": 1,
        "HourlyNotionalCost(£)": 2.55,
        "IncidentNotionalCost(£)": 2.55,
    },
]
LATIN1_AFTER_FIRST_2KB = ("just_one_column\n" + "aabbcc\n" * 1048 + "a.b.é").encode(
    "latin-1"
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "filename,content,expected_url,expected_rows",
    (
        ("dogs.csv", SIMPLE, "/data/dogs", SIMPLE_EXPECTED),
        (
            "weird ~ filename here.csv.csv",
            SIMPLE,
            '/data/weird+~7E+filename+here~2Ecsv',
            SIMPLE_EXPECTED,
        ),
        ("not-utf8.csv", NOT_UTF8, "/data/not-utf8", NOT_UTF8_EXPECTED),
        ("latin1-after-x.csv", "LATIN1_AFTER_FIRST_2KB", "/data/latin1-after-x", ANY),
    ),
)
@pytest.mark.parametrize("use_xhr", (True, False))
async def test_upload(tmpdir, filename, content, expected_url, expected_rows, use_xhr):
    path = str(tmpdir / "data.db")
    db = sqlite_utils.Database(path)
    db.vacuum()
    db.enable_wal()
    binary_content = content
    # Trick to avoid a 12MB string being part of the pytest rendered test name:
    if content == "LATIN1_AFTER_FIRST_2KB":
        binary_content = LATIN1_AFTER_FIRST_2KB

    db["hello"].insert({"hello": "world"})

    datasette = Datasette([path])

    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}

    # First test the upload page exists
    async with httpx.AsyncClient(app=datasette.app()) as client:
        response = await client.get("http://localhost/-/upload-csvs", cookies=cookies)
        assert 200 == response.status_code
        assert (
            '<form action="/-/upload-csvs" id="uploadForm" method="post"'
            in response.text
        )
        csrftoken = response.cookies["ds_csrftoken"]
        cookies["ds_csrftoken"] = csrftoken

        # Now try uploading a file
        files = {"csv": (filename, binary_content, "text/csv")}
        response = await client.post(
            "http://localhost/-/upload-csvs{}".format(
                "?_num_bytes_to_detect_with=2048"
                if content == "LATIN1_AFTER_FIRST_2KB"
                else ""
            ),
            cookies=cookies,
            data={"csrftoken": csrftoken, "xhr": "1" if use_xhr else ""},
            files=files,
        )
        if use_xhr:
            assert response.json()["url"] == expected_url
        else:
            assert "<h1>Upload in progress</h1>" in response.text
            assert expected_url in response.text

        # Now things get tricky... the upload is running in a thread, so poll for completion
        fail_after = 20
        iterations = 0
        while True:
            response = await client.get(
                "http://localhost/data/_csv_progress_.json?_shape=array"
            )
            rows = json.loads(response.content)
            assert 1 == len(rows)
            row = rows[0]
            assert row["table_name"] == filename[:-4]
            assert not row["error"], row
            if row["bytes_todo"] == row["bytes_done"]:
                break
            iterations += 1
            assert iterations < fail_after, "Took too long: {}".format(row)
            await asyncio.sleep(0.5)

    rows = list(db[filename[:-4]].rows)
    assert rows == expected_rows


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
