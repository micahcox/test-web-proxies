import aiohttp
# import aiodns  <-- be sure aiodns is installed.  It's used by aiohttp to speed up DNS lookups.
import asyncio
import aiofiles
import logging
import signal
import uuid
import sys
import copy
import getpass
import time
from bs4 import BeautifulSoup # pip install BeautifulSoup4


# Exception handling for asyncio loop.
def handle_exception(loop, context: dict):
    #msg = context.get("exception", context["message"])
    exception_object = context.get("exception")
    if exception_object:
        exception_name = type(exception_object).__name__
    else:
        exception_name = "<CAN'T GET EXCEPTION NAME>"

    msg = context.get("message")
    logging.error(f"Caught exception '{exception_name}': {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(loop))


async def shutdown(loop, signal = None):
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    
    # Make a list of tasks, not including this shutdown task.
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    
    # Cancel all tasks.
    [task.cancel() for task in tasks]
    
    logging.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(15)
    # Stopping AsyncIO loop.
    #loop.stop()


class WorkingURL:
    def __init__(self, url_id: str, domain: str, url: str):
        self.url_id = url_id
        self.domain = domain
        self.url = url
        self.title = None
        self.response = None
        self.status = None


# Make a list of potential URLs from domain.    
def make_url_list(domain: str):
    protocols = ["https"]
    hostnames = ["www"]
 
    return (f"{protocol}://{hostname}.{domain}" for protocol in protocols for hostname in hostnames)


def extract_page_data(url: WorkingURL):
    try:
        if url:
            if url.response_text:
                soup = BeautifulSoup(url.response_text, "html.parser")
            
                title_tag = soup.find("title")
                rating_meta = soup.find("meta", attrs={"name": "rating"})
                
                if rating_meta:
                    url.rating = rating_meta["content"] if rating_meta["content"].isprintable() else None
                else:
                    url.rating = None
                    
                if title_tag:
                    url.title = title_tag.text if title_tag.text.isprintable() else None
                else:
                    url.title = None
    except TypeError as err:
        logging.error(f"extract_page_data - TypeError exception {err}")
    except Exception as err:
        logging.error(f"extract_page_data: unknown Exception {type(err).__name__} - {err}")


async def get_page(url: WorkingURL, session: aiohttp.ClientSession, return_body = False,  queue: asyncio.Queue = None,  proxy_url: str = None):   
    if proxy_url:
        logging.debug(f'get_page: Proxy parameters passed are proxy_url: {proxy_url}. WorkingURL object type is: {type(url)}. session type is: {type(session)}. queue type: {type(queue)}. url.url type is: {type(url.url)}')
    else:
        logging.debug(f'get_page: Non-Proxy parameters passed are proxy_url: {proxy_url}. WorkingURL object type is: {type(url)}. session type is: {type(session)}. queue type: {type(queue)}. url.url type is: {type(url.url)}') 

    try:
        
            async with session.get(url.url, proxy = proxy_url) as response:

                # url.response = response   # Don't hang on to this object or close() may not free resources.  Copy values instead
                url.response_url = None
                url.status = response.status
                url.title = None
                url.response_text = None

                if response.status == 200:
                    url.response_url = response.url
                    logging.debug(f"{url.url_id} - {response.status} OK - {response.url}")
                else:
                    logging.debug(f"{url.url_id} - response code {response.status} - {response.url}")


                if response.status >= 500:
                    logging.error(f"{url.url_id} - get_page: {response.status} - {response.reason} - {response.url}")
                    response.close()
                    return None
                    
                if return_body:
                    # Pages that can return some type of body and title should be processed here.
                    if response.status in (200, 401, 403, 451):
                        logging.debug(f"{url.url_id} - Status Code: {response.status} - getting html page from URL - {response.url}")
                        
                        url.response_text = await response.text()
                        url.response_url = response.url
                        
                        extract_page_data(url)

                        # Get page title
                        if url.title:
                            logging.debug(f"{url.url_id} - Status Code: {response.status} - Title found for {url.url}, queued for further processing.") 

                            if queue:
                                # Queue good page for processing
                                #time.sleep(1)
                                asyncio.create_task(queue.put(url))
                        else:
                            logging.debug(f"{url.url_id} - Status Code: {response.status} - Title not found in page for url: {response.url}") 
                    else:
                        logging.debug(f"{url.url_id} - response status {response.status} so not attempting to retrieve html page from URL - {response.url}")        

                if response:
                    # Close the response to recover resources
                    response.close()

    except aiohttp.ClientHttpProxyError: 
        pass
        # Getting UnboundLocalError when this code runs.  Seems to be a referense to "response"
        #logging.debug(f"{url.url_id} get_page: aiohttp library threw ClientHttpProxyError for proxy {proxy_url} for url {url.url}")
        #response.close()

    except aiohttp.InvalidURL as err:
        if proxy_url:
            logging.error(f"{url.url_id} - get_page: aiohttp.InvalidURL exception - proxy URL: {proxy_url} - Page URL: {url.url} - {err}")
        else:
            logging.error(f"{url.url_id} - get_page: aiohttp.InvalidURL exception - direct access Page URL: {url.url} - {err}")
            
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"get_page - TypeError exception {err}")

    except aiohttp.ClientConnectionError as err:
        logging.error(f"get_page: {url.url_id} - Connection error - {url.domain} - {url.url} - {err}")

    except asyncio.CancelledError as err:     
        logging.info(f"get_page: {url.url_id} - Get page cancelled - {url.domain} - {url.url} - {err}")

    except Exception as err:
        logging.error(f"get_page: unknown Exception {type(err).__name__} - {err}")


# Queues a series of URL objects built from a list of domains, for later processing.       
async def make_urls(test_queue, filename: str):
    try:
        async with aiofiles.open(filename, mode = "rt") as domains:
            async for raw_domain in domains:
                domain = raw_domain.strip().lower()
                logging.debug(f"Queuing URLs for domain {domain}") 

                # queue an async task for various combinations of domain and protocol
                
                for url in make_url_list(domain):
                    url_id = str(uuid.uuid4()) # UUID for each URL
                    url = WorkingURL(url_id = url_id, domain = domain, url = url)
                    asyncio.create_task(test_queue.put(url))
                    logging.debug(f"{url.url_id} for {url.url} queued.")

    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"make_urls - TypeError exception {err}")
        return

    except NotImplementedError as err:
        logging.error(f"make_urls - NotImplementedError exception.  Exiting make_urls. - {err}")
        return

    except (OSError, FileExistsError, FileNotFoundError) as err:
        # Log right now.  Catestrophic error causing exit of task
        logging.error(f"make_urls - Problem with reading file {filename}. Exiting async file make_urls task. {err}")
        return


async def test_urls(test_queue, good_queue, session: aiohttp.ClientSession):
    try:
        while True:
            url = await test_queue.get()    
            logging.info(f"Getting page for {url.url}")
            asyncio.create_task(get_page(url, session, return_body = True, queue=good_queue))

    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"test_urls - TypeError exception {err}")
        return
    except Exception as err:
        logging.error(f"test_urls: unknown Exception {type(err).__name__} - {err}")


# Subscriber writes page data to CSV file. 
async def write_good_page_info(queue: asyncio.Queue, filename: str):
    try:
        async with aiofiles.open(filename, mode = "w", encoding="utf-8", newline="") as page_info_file:
            while True:
                url = await queue.get() # Pull WorkingURL object and write data to file.

                # Write "Good" pages only.
                if url.status == 200:
                    logging.debug(f"{url.url_id} write_good_page_info: Writing page info for - {url.url}")
                    await page_info_file.write(f'"{url.url_id}","{url.status}","{url.domain}","{url.url}","{url.response_url}","{url.title}","{url.rating}"\n')
                    # Windows not flushing on unclean shutdown.  Also not honoring signals so flushing to get lines to write
                    if sys.platform == "win32":
                        await page_info_file.flush()

                    # DO NOT USE "create_task()" to write line. mcox 2/7/2023
                    # Creating task with writer puts random binary in file.  Probably trashes write pointer.
                    # asyncio.create_task(write_csv_line(page_info_file, url))
                else: 
                    logging.debug(f"{url.url_id} write_good_page_info: NOT writing page info for status {url.status} - {url.url}")
    except AttributeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_good_page_info - Attribute exception {err}")
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_good_page_info - TypeError exception {err}")
        return      
    except (OSError, FileExistsError, FileNotFoundError) as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_good_page_info - Problem with writing file {filename}. Exiting async file writing task. {err}")
        return
    except Exception as err:
        logging.error(f"write_good_page_info: unknown Exception {type(err).__name__} - {err}")


# Subscriber writes direct and proxy page data to CSV file for comparison.
async def write_proxy_page_info(queue: asyncio.Queue, filename: str):
    try:
        async with aiofiles.open(filename, mode = "w", encoding="utf-8", newline="") as page_info_file:
            while True:
                urls = await queue.get() # Pull WorkingURL object and write data to file.
                url = urls.get("url") # WorkingURL good direct page
                proxy_urls = urls.get("proxy_urls") # List of proxy servers and WorkingURL page results from each

                logging.debug(f"{url.url_id} write_proxy_page_info: Writing proxy page info for comparison.")

                line = f'"{url.url_id}","{url.domain}","{url.url}","{url.response_url}","{url.status}","{url.title}"'
                
                for p_url in proxy_urls:
                    line += f',"{p_url.status}","{p_url.title}"'

                await page_info_file.write(f'{line}\n')
                # Windows not flushing on unclean shutdown.  Also not honoring signals so flushing to get lines to write
                if sys.platform == "win32":
                    await page_info_file.flush()
                
                # DO NOT USE "create_task()" to write line. mcox 2/7/2023
                # Creating task with writer puts random binary in file.  Probably trashes write pointer.
                # asyncio.create_task(write_csv_line(page_info_file, url))

    except AttributeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_proxy_page_info - Attribute exception {err}")
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_proxy_page_info - TypeError exception {err}")
        return      
    except (OSError, FileExistsError, FileNotFoundError) as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_proxy_page_info - Problem with writing file {filename}. Exiting async file writing task. {err}")
        return
    except Exception as err:
        logging.error(f"write_proxy_page_info: unknown Exception {type(err).__name__} - {err}")


# Subscriber compares direct page and proxy pages to see if proxies are similar 
# to each other.  Direct page is baseline "working" page.
# Compare proxy1 and proxy2 return info and report on differences.
async def get_url_from_proxy_list(url: WorkingURL, proxy_info: list, queue: asyncio.Queue, session: aiohttp.ClientSession):
    try:
        logging.debug(f"get_url_from_proxy_list: Retrieving {url.url} via proxy servers.")

        proxy_status = []
        proxy_titles = []
        new_urls = [] # List of WorkingURL objects, one for each proxy

        # Get the good page in "url" WorkingURL argument for all of the proxy servers in the list
        for index,proxy in enumerate(proxy_info):
            new_urls.append(WorkingURL(url.url_id, url.domain, url.url))
            await get_page(new_urls[index], session, return_body = True, proxy_url = proxy.get("proxyurl")) 

            if hasattr(new_urls[index], "status"):
                    logging.debug(f'get_url_from_proxy_list: Proxy Response code: {new_urls[index].status} Proxy: {proxy.get("proxyurl")} - {new_urls[index].url}')
                    proxy_status.append(new_urls[index].status)
                    proxy_titles.append(new_urls[index].title)
            else:
                proxy_status.append(None)
                proxy_titles.append(None)
                logging.debug(f'get_url_from_proxy_list: no response object returned for proxy {proxy.get("proxyurl")} for URL {new_urls[index].url}')
        
        
        # Queue the page results for further processing
        urls = {
            "url": url,
            "proxy_urls": new_urls
        }

        asyncio.create_task(queue.put(urls))
        logging.debug(f'{url.url_id}: Status Codes: {url.status} {proxy_status} - Titles: "{url.title}", {proxy_titles} for {url.url}')

    except AttributeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"get_url_from_proxy_list - Attribute exception {err}")
        return        
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"get_url_from_proxy_list - TypeError exception {err}")
        return
    except Exception as err:
        logging.error(f"get_url_from_proxy_list: unknown Exception {type(err).__name__} - {err}")


# Compare good page from direct Internet, proxy1, proxy2 proxies
async def get_pages_from_proxies(in_queue: asyncio.Queue, out_queue: asyncio.Queue, session: aiohttp.ClientSession):

    try: 
        username = ""
        password = ""

        proxy_info = [
                        {
                            "proxyurl": f"http://{username}:{password}@204.99.62.249:8080", # Proxy 1
                            "port": 8080,
                            "session": None,
                            "username": None,
                            "password": None
                        },
                        {
                            "proxyurl": f"http://{username}:{password}@scdp-proxy-004.corp.cvscaremark.com:9119",  # Proxy2
                            "port": 9119,
                            "session": None,
                            "username": None,
                            "password": None
                        }
                    ]

        while True:
            url: WorkingURL = await in_queue.get() # Get a working candidate page to test against proxy servers                    
            logging.debug(f"get_pages_from_proxies: comparing proxy server pages for {url.url}")
            asyncio.create_task(get_url_from_proxy_list(url, proxy_info, out_queue, session))

    except AttributeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"get_pages_from_proxies - Attribute exception {err}")
        return 
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"get_pages_from_proxies: - TypeError exception {err}")
        return
    except Exception as err:
        logging.error(f"get_pages_from_proxies: unknown Exception {type(err).__name__} - {url.domain} - {url.url} - {err}")

# Get WorkingURL object from in_queue, publish to list of out_queues
async def publisher(in_queue: asyncio.Queue, out_queues):
    try:
        while True:
            url = await in_queue.get()
            for queue in out_queues:
                asyncio.create_task(queue.put(url))
    except AttributeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"publisher - Attribute exception {err}")
        return   
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        # Note: copy.deepcopy() on url object produced this error
        #     TypeError exception can't pickle multidict._multidict.CIMultiDictProxy objects
        logging.error(f"publisher: - TypeError exception {err}")
        return
    except Exception as err:
        logging.error(f"publisher: unknown Exception {type(err).__name__} - {err}")


""" BROKEN: Writes records too many times to file
# Write line to open file.  Launch as task for max asyncio concurrency.
async def write_csv_line(csv_writer: aiocsv.AsyncWriter, url: WorkingURL):
    await csv_writer.writerow([url.url_id, url.status, url.domain, url.url, url.response_url, url.title, url.rating])
"""

""" BROKEN: Random binary appears when this is called as a task.  An await in the caller works.  The write pointer probably gets trashed.
async def write_csv_line(file_handle, url: WorkingURL):
    await file_handle.write(f'"{url.url_id}","{url.status}","{url.domain}","{url.url}","{url.response_url}","{url.title}","{url.rating}"\n')
"""

                       
def main():
    
    # AsyncIO event loop setup
    try:   
        # If running windows, bad things happen such as aiofiles not opening file so SelectorEventLoop needed
        if sys.platform == 'win32':
            #asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            #asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy)
            pass

        #loop = asyncio.get_event_loop() # DEPRECATED.  Doesn't work in Python >= v3.10 for creating new loop
        loop = asyncio.new_event_loop()
        
        if sys.platform != 'win32':
            # Signal handler for more graceful shutdown at any point during runtime. 
            #Build list of supported signals for environment
            signals = (x for x in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT) if x._name_ in dir(signal))

            #  add_signal_handler Not implemented in Windows but is supported on *NIX and works well.
            try:
                for s in signals:
                    loop.add_signal_handler(
                        # signal=signal fixes late binding with lambda.  Google it.
                        s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
                    )
                signal_handler_attached = True
                
            except NotImplementedError:
                signal_handler_attached = False
                pass

        
        loop.set_exception_handler(handle_exception)

        # Schedule URL work
        loop.create_task(start_pubsub(loop)) 
        # Start loop 
        loop.run_forever()
    except Exception as err:
        logging.error(f"main: unknown Exception {type(err).__name__} - {err}")
    finally:
        # STOP!!!!
        asyncio.create_task(shutdown(loop))
        loop.close()
        logging.info("Successfully shutdown.")

async def start_pubsub(loop):
    DOMAIN_FILENAME = "domains.txt"
    PAGEINFO_FILENAME = "page_info.csv"
    PROXY_PAGEINFO_FILENAME = "proxy_page_info.csv"
    #aiohttp session setup

    DNS_CACHE_SECONDS = 10 # Default is 10 seconds.  Decrease if using millions of domains.
    PARALLEL_CONNECTIONS = 3000 # Simultaneous TCP connections tracked by aiohttp session
    PER_HOST_CONNECTIONS = 5 # Maximum connections per host.  Keep low.  Default is infinite.



    try:
        # Publisher queue chain
        url_queue = asyncio.Queue() # WorkingURL objects with candidate URLs to get good pages
        good_page_queue = asyncio.Queue() # WorkingURL objects after page read successfully.  Consumed to write CSV file.

        # Subscriber queues.  Sends references to WorkingURL objects
        csv_queue = asyncio.Queue() # subscriber queue of WorkingURL objects for csv writer
        proxy_queue = asyncio.Queue() # subscriber queue of WorkingURL objects to get proxy server pages from list of proxies
        write_proxy_results_queue = asyncio.Queue() # Write proxy server query results
        #subscriber_queues = (csv_queue, proxy_queue) # For publisher coro to distribute good pages to tasks
        subscriber_queues = (proxy_queue,) # For publisher coro to distribute good pages to tasks

        direct_tcp_connector = aiohttp.TCPConnector(verify_ssl=True, limit = PARALLEL_CONNECTIONS,
                                                    limit_per_host = PER_HOST_CONNECTIONS, ttl_dns_cache = DNS_CACHE_SECONDS)
        client_timeout = aiohttp.ClientTimeout(total=120.0, connect = 60.0, sock_connect = 30.0, sock_read = 30.0)
        
        session = aiohttp.ClientSession(connector = direct_tcp_connector, timeout = client_timeout)

        
        # Distribute deep copies of good pages to processing queues.
        loop.create_task(publisher(good_page_queue, subscriber_queues)) 
        # Build and publish URLs from domain list.  
        loop.create_task(make_urls(url_queue, DOMAIN_FILENAME)) 
        # Test URLs and publish "good" pages. 200 OK, with title, etc.
        loop.create_task(test_urls(url_queue, good_page_queue, session)) #
        # Write good URL data to CSV file.
        #loop.create_task(write_good_page_info(csv_queue, PAGEINFO_FILENAME))
        # Get pages from a list of proxy servers from queue of "good" directly loaded pages
        loop.create_task(get_pages_from_proxies(proxy_queue, write_proxy_results_queue, session))
        # Write normal and Proxy page results per line for comparison.
        loop.create_task(write_proxy_page_info(write_proxy_results_queue, PROXY_PAGEINFO_FILENAME))
             
    except Exception as err:
        logging.error(f"main: unknown Exception {type(err).__name__} - {err}")

if __name__ == '__main__':    
 
    # Global semaphore to constrain parallel url gets.
    SEMAPHORE_LIMIT = 100
    #sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    # Set up logging
    #LOG_LEVEL = logging.ERROR
    LOG_LEVEL = logging.WARN
    #LOG_LEVEL = logging.INFO
    #LOG_LEVEL = logging.DEBUG

    LOG_FMT_STR = "%(asctime)s,%(msecs)d %(levelname)s: %(message)s"
    LOG_DATEFMT_STR = "%H:%M:%S"

    logging.basicConfig(level=LOG_LEVEL, format=LOG_FMT_STR, datefmt=LOG_DATEFMT_STR)
    
    main()
    

