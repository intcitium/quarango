import unittest
import asyncio
from loguru import logger
from apiserver.blueprints.admin.views import home


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class TestAdminRoutes(unittest.TestCase):

    def test_home_2(self):
        r = (_run, home())
        print(r)


if __name__ == '__main__':
    unittest.main()
