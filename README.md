# datasette-upload-csvs

[![PyPI](https://img.shields.io/pypi/v/datasette-upload-csvs.svg)](https://pypi.org/project/datasette-upload-csvs/)
[![CircleCI](https://circleci.com/gh/simonw/datasette-upload-csvs.svg?style=svg)](https://circleci.com/gh/simonw/datasette-upload-csvs)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/datasette-upload-csvs/blob/master/LICENSE)

Datasette plugin for uploading CSV files and converting them to a database table

## Installation

    pip install datasette-upload-csvs

This plugin does not implement authentication, so if you are going to run this on a public site you should use something like [datasettte-auth-github](https://github.com/simonw/datasette-auth-github) to ensure only authenticated users can interact with Datasette and upload data to it.
