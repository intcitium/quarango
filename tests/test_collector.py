import unittest
from loguru import logger
from collector import web_driver


class TestAdminRoutes(unittest.TestCase):

    def test_home_2(self):

        web_driver.scroll()


if __name__ == '__main__':
    unittest.main()
