from quart import Quart
from apiserver.blueprints.admin import admin


def create_app():
    app = Quart(__name__)
    app.register_blueprint(admin)
    return app
