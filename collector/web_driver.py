import platform
import requests
import os
import time
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import selenium.common.exceptions as scroll_errors
from networkx import DiGraph

cds = 'https://chromedriver.storage.googleapis.com/index.html?path=81.0.4044.138/'
win = '%schromedriver_win32.zip' % cds
mac = '%schromedriver_mac64.zip' % cds
lnx = '%schromedriver_linux64.zip' % cds
cd_exe = 'chromedriver.exe'
driver_path = os.path.join(os.path.dirname(__file__), cd_exe)

if cd_exe not in os.listdir(os.path.dirname(__file__)):
    this_platform = {'os': platform.system(), 'ver': platform.release(), 'cpu': platform.machine()}
    if this_platform['os'].upper() == 'WINDOWS':
        driver_zip = requests.get(win, stream=True)
    elif this_platform['os'].upper() == 'LINUX':
        driver_zip = requests.get(lnx, stream=True)
    else:
        driver_zip = requests.get(mac, stream=True)
    # Need this to extract but it's not TODO
    with open(driver_path, 'wb') as fd:
        for chunk in driver_zip.iter_content(chunk_size=500):
            fd.write(chunk)

else:
    print("%s exists" % driver_path)


def scroll(webdriver_path=driver_path, timeout=3, graph=DiGraph(), search_ids=None):
    """
    Use a more complex method to gather data that uses a web driver to scrape a page. It must go to the page and then
    scroll to the bottom so it can gather all the posts, their authors and dates published so it can also be turned into
    a graph
    Parameters
    ----------
    webdriver_path (str)
        where the chrome web driver is stored for establishing the driver
    timeout (int)
        how many seconds the driver should wait for the page to complete the re-load when scrolling
    Returns
    -------
    """
    # Driver is currently set for version 8.1 on windows
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(executable_path=webdriver_path, chrome_options=chrome_options)
    # Node and Edge containers which will be returned starting with the base site which is being collected
    index = ['mediumcom']
    if not graph.has_node('mediumcom'):
        graph.add_node('mediumcom', description='Site with blogs')
    # Start the driver on the url and the query if it exists.
    if not search_ids:
        search_ids = ['network%20graph%20visualization']
    elif isinstance(search_ids, list):
        # Create a search that consists of all the terms and put it at the beginning of the list
        if len(search_ids) > 1:
            search_ids.insert(0, '%20'.join(search_ids))
    else:
        search_ids = [search_ids]
    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")
    driver.find_elements_by_class_name('postArticle')
    for search_id in search_ids:
        # Set the driver on the search_id in the query
        driver.get('https://medium.com/search?q=%s' % search_id)
        # Normalize the ID now that the url is set
        search_id = search_id.replace('%20', '_')
        if not graph.has_node(search_id):
            graph.add_node(search_id, description='Search term used to search blogs')
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Wait to load page
            logger.info(
                'Collected %d posts. Scrolling for more...' % len(driver.find_elements_by_class_name('postArticle')))
            time.sleep(timeout)
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height or len(driver.find_elements_by_class_name('postArticle')) > 100:
                # If heights are the same it will exit the function
                break
            last_height = new_height
        # Collect all the posts to iterate through and assign to nodes and edges
        posts = driver.find_elements_by_class_name('postArticle')
        logger.info('Collected %s posts' % len(posts))
        # Go through each post and extract an author (a_id), the post (b_id) and then create the edges
        for post in posts:
            try:
                author = post.find_element_by_class_name('ds-link').text
                link = post.find_element_by_class_name('ds-link').get_attribute("href")
                date = post.find_element_by_tag_name('time').text
                title = post.find_element_by_tag_name('h3').text
                claps = post.find_element_by_class_name('multirecommend').text
                # Create the author node
                a_id = ''.join(e for e in author if e.isalnum()).lower()
                if a_id not in index:
                    graph.add_node(a_id, description=author, link=link)
                    index.append(a_id)
                # Create the article node
                b_id = ''.join(e for e in title if e.isalnum()).lower()
                if b_id not in index:
                    graph.add_node(b_id, description="%s by %s" % (title, author), link=link, count=claps, date=date)
                    index.append(b_id)
                    graph.has_node(a_id)
                if graph.has_node(a_id) and graph.has_node(b_id):
                    graph.add_edge(a_id, b_id, label='Posted')
                    graph.add_edge(b_id, 'mediumcom', label='PostedOn')
                    graph.add_edge(search_id, b_id, label='FromSearch')
            except scroll_errors.NoSuchElementException as error:
                logger.error("Scrolling %s" % error.msg)

    return graph
