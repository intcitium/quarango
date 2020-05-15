import asyncio
import threading
from quart import Blueprint, jsonify, websocket, request

from apiserver.blueprints.admin.models import get_suggestions, get_neighbors
from collector.web_driver import scroll


admin = Blueprint('admin', __name__)


@admin.route('/')
async def home():
    data = await request.get_json()
    return jsonify(response=200, message="Base end point", data=data)


@admin.route('/get_suggestion_items', methods=['POST'])
async def get_suggestion_items():
    req = (await request.form).to_dict()['searchterms']
    data = get_suggestions(search_term=req)
    # TODO Kick off thread for collection based on search
    crawl = threading.Thread(
        target=scroll, kwargs={'search_ids': [req]})
    crawl.start()
    return jsonify(
        response=200,
        message="Search for {req} resulted in {count} items".format(req=req, count=len(data)),
        data=data)


@admin.route('/get_neighbors_index', methods=['POST'])
async def get_neighbors_index():
    req = (await request.form).to_dict()['nodekey']
    data = await get_neighbors(node_key=req)
    return jsonify(
        response=200,
        message="{count} neighbors found for {req}".format(count=len(data['nodes']), req=req),
        data=data)


@admin.route('/get_shortest_path', methods=['POST'])
async def get_shortest_path():
    form = (await request.form).to_dict()['nodekey']
    data = get_neighbors(node_key=form)
    return jsonify(response=200, message="Non Async for search fields", data=data)


@admin.websocket("/ws")
async def ws():
    print(websocket.headers)
    while True:
        try:
            data = await websocket.receive()
            await websocket.send(f"Echo {data}")
        except asyncio.CancelledError:
            # Handle disconnect
            raise
