from datasette import hookimpl
from .app import UploadApp


@hookimpl
def asgi_wrapper(datasette):
    def wrap_with_asgi_auth(app):
        async def wrapped_app(scope, recieve, send):
            if scope["path"] == "/-/upload-csv":
                await UploadApp(scope, recieve, send, datasette)
            else:
                await app(scope, recieve, send)

        return wrapped_app

    return wrap_with_asgi_auth
