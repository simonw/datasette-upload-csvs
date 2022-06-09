from setuptools import setup
import os

VERSION = "0.7.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="datasette-upload-csvs",
    description="Datasette plugin for uploading CSV files and converting them to database tables",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/simonw/datasette-upload-csvs",
    license="Apache License, Version 2.0",
    version=VERSION,
    packages=["datasette_upload_csvs"],
    entry_points={"datasette": ["upload_csvs = datasette_upload_csvs"]},
    python_requires=">=3.7",
    install_requires=[
        "datasette>=0.61",
        "asgi-csrf>=0.7",
        "starlette",
        "aiofiles",
        "python-multipart",
        "charset-normalizer",
        "sqlite-utils",
    ],
    extras_require={
        "test": ["pytest", "pytest-asyncio", "asgiref", "httpx", "asgi-lifespan"]
    },
    package_data={"datasette_upload_csvs": ["templates/*.html"]},
)
