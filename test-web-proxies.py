import aiohttp
# import aiodns  <-- be sure aiodns is installed.  It's used by aiohttp to speed up DNS lookups.
import asyncio
import aiofiles
import logging
import signal
import uuid
import sys
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
    # Stopping AsyncIO loop.
    loop.stop()

class WorkingURL:
    def __init__(self, url_id: str, domain: str, url: str):
        self.url_id = url_id
        self.domain = domain
        self.url = url


# Make a list of potential URLs from domain.    
def make_url_list(domain: str):
    protocols = ["http", "https"]
    hostnames = ["www"]
 
    return (f"{protocol}://{hostname}.{domain}" for protocol in protocols for hostname in hostnames)

def extract_page_data(url: WorkingURL):
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
    

async def get_page(good_queue: asyncio.Queue, url: WorkingURL, session: aiohttp.ClientSession, return_body = False):   
    try:
        async with session.get(url.url) as response:
            
            url.response: aiohttp.ClientResponse = response
            url.response_text = None
            
            if response.status == 200:
                logging.debug(f"{url.url_id} - {response.status} OK - {response.url}")
            else:
                logging.debug(f"{url.url_id} - response code {response.status} - {response.url}")
                
            if response.status == 403:  # Forbidden
                #TODO: Common websites return this.  Find out why and respond accordingly.  Could be SSL or other issue.
                logging.error(f"{url.url_id} - {response.status} - {response.reason} - {response.url}")
                response.close()
                return None
            
            if response.status >= 400:
                logging.error(f"{url.url_id} - {response.status} - {response.reason} - {response.url}")
                response.close()
                return None
            
            if return_body:
                if response.status == 200:
                    logging.debug(f"{url.url_id} - getting html page from URL - {response.url}")
                    url.response_text = await response.text()
                    
                    extract_page_data(url)

                    # Get page title
                    if url.title:
                        logging.debug(f"{url.url_id} 200 OK and Title found for {url.url}, queued for further processing.") 
                        # Queue good page for processing
                        asyncio.create_task(good_queue.put(url))
                    else:
                        logging.debug(f"{url.url_id} - Title not found in page for url: {response.url}") 
                else:
                    logging.debug(f"{url.url_id} - response status {response.status} so not attempting to retrieve html page from URL - {response.url}")        
            # Explicitly close the session    
            response.close()
                       
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"get_page - TypeError exception {err}")
        return

    except aiohttp.ClientConnectionError as err:
        logging.error(f"get_page: {url.url_id} - Connection error - {url.domain} - {url.url} - {err}")

    except asyncio.CancelledError as err:     
        logging.info(f"get_page: {url.url_id} - Get page cancelled - {url.domain} - {url.url} - {err}")
        response.close()
    
    except Exception as err:
        logging.error(f"{url.url_id} - get_page unknown Exception - {url.domain} - {url.url} - {err}")


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


async def test_urls(test_queue, good_queue):
    try:
        # Setup aiohttp sessions
        DNS_CACHE_SECONDS = 10 # Default is 10 seconds.  Decrease if using millions of domains.
        PARALLEL_CONNECTIONS = 300 # Simultaneous TCP connections tracked by aiohttp session
        PER_HOST_CONNECTIONS = 5 # Maximum connections per host.  Keep low.  Default is infinite.
        
        # aiohttp ssl, proxy, and connection docs: https://docs.aiohttp.org/en/stable/client_advanced.html#client-session
        # See the following for proxy TLS in TLS: https://github.com/aio-libs/aiohttp/discussions/6044#discussioncomment-1432443
        
        direct_tcp_connector = aiohttp.TCPConnector(verify_ssl=True, limit = PARALLEL_CONNECTIONS,
                                                    limit_per_host = PER_HOST_CONNECTIONS, ttl_dns_cache = DNS_CACHE_SECONDS)
        
        async with aiohttp.ClientSession(connector = direct_tcp_connector) as session:
            while True:
                url = await test_queue.get()
                logging.info(f"Getting page for {url.url}")
                asyncio.create_task(get_page(good_queue, url, session, return_body = True))
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"test_urls - TypeError exception {err}")
        return


# Subscriber writes page data to CSV file. 
async def write_page_info(queue, filename):
    try:
        async with aiofiles.open(filename, mode = "w", encoding="utf-8", newline="") as page_info_file:
            while True:
                url = await queue.get() # Pull WorkingURL object and write data to file.
                logging.info(f"Writing page info for {url.url_id} - {url.url}")
                await page_info_file.write(f'"{url.url_id}","{url.response.status}","{url.domain}","{url.url}","{url.response.url}","{url.title}","{url.rating}"\n')
                
                # DO NOT USE "create_task()" to write line. mcox 2/7/2023
                # Creating task with writer puts random binary in file.  Probably trashes write pointer.
                # asyncio.create_task(write_csv_line(page_info_file, url))
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_page_info - TypeError exception {err}")
        return      
    except (OSError, FileExistsError, FileNotFoundError) as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"write_page_info - Problem with writing file {filename}. Exiting async file writing task. {err}")
        return

# Subscriber compares direct page and proxy pages to see if proxies are similar 
# to each other.  Direct page is baseline "working" page.
# Compare Forcepoint and Skyhigh (Mcafee) return info and report on differences.
async def compare_proxy_page(url: WorkingURL, session: aiohttp.ClientSession):
    # await Get page from forcepoint
    # await Get page from McAfee
    # Compare and report differences
    try:
        logging.debug(f"compare_proxy_page: TODO: {url.url}")
        pass
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"compare_proxy_page - TypeError exception {err}")
        return


# Compare good page from direct Internet, Forcepoint, Skyhigh proxies
async def compare_proxy_servers(queue: asyncio.Queue):
    try: 
        # Setup aiohttp sessions
        DNS_CACHE_SECONDS = 10 # Default is 10 seconds.  Decrease if using millions of domains.
        PARALLEL_CONNECTIONS = 300 # Simultaneous TCP connections tracked by aiohttp session
        PER_HOST_CONNECTIONS = 5 # Maximum connections per host.  Keep low.  Default is infinite.

        # aiohttp ssl, proxy, and connection docs: https://docs.aiohttp.org/en/stable/client_advanced.html#client-session
        # See the following for proxy TLS in TLS: https://github.com/aio-libs/aiohttp/discussions/6044#discussioncomment-1432443
        proxy_tcp_connector = aiohttp.TCPConnector(verify_ssl=True, limit = PARALLEL_CONNECTIONS,
                                                limit_per_host = PER_HOST_CONNECTIONS, ttl_dns_cache = DNS_CACHE_SECONDS)

        async with aiohttp.ClientSession(connector = proxy_tcp_connector) as session:
            while True:
                url: WorkingURL = await queue.get()
                logging.debug(f"compare_proxy_servers: comparing proxy server pages for {url.url}")
                asyncio.create_task(compare_proxy_page(url, session))
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        logging.error(f"compare_proxy_servers: - TypeError exception {err}")
        return

# Get WorkingURL object from in_queue, publish to list of out_queues
async def publisher(in_queue: asyncio.Queue, out_queues):
    try:
        while True:
            url = await in_queue.get()
            for queue in out_queues:
                asyncio.create_task(queue.put(url))
    
    except TypeError as err:
        # Write catestrophic error immediately instead of queuing
        # Note: copy.deepcopy() on url object produced this error
        #     TypeError exception can't pickle multidict._multidict.CIMultiDictProxy objects
        logging.error(f"publisher: - TypeError exception {err}")
        return


""" BROKEN: Writes records too many times to file
# Write line to open file.  Launch as task for max asyncio concurrency.
async def write_csv_line(csv_writer: aiocsv.AsyncWriter, url: WorkingURL):
    await csv_writer.writerow([url.url_id, url.response.status, url.domain, url.url, url.response.url, url.title, url.rating])
"""

""" BROKEN: Random binary appears when this is called as a task.  An await in the caller works.  The write pointer probably gets trashed.
async def write_csv_line(file_handle, url: WorkingURL):
    await file_handle.write(f'"{url.url_id}","{url.response.status}","{url.domain}","{url.url}","{url.response.url}","{url.title}","{url.rating}"\n')
"""

                       
def main():
    # AsyncIO event loop setup
    #DEPRECATED
    #loop = asyncio.get_event_loop()

    # If running windows, bad things happen such as aiofiles not opening file so SelectorEventLoop needed
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = asyncio.new_event_loop()
    
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

    # Publisher queue chain
    url_queue = asyncio.Queue() # WorkingURL objects with candidate URLs to get good pages
    good_page_queue = asyncio.Queue() # WorkingURL objects after page read successfully.  Consumed to write CSV file.

    # Subscriber queues.  Sends references to WorkingURL objects
    csv_queue = asyncio.Queue() # subscriber queue of WorkingURL objects for csv writer
    proxy_queue = asyncio.Queue() # subscriber queue of WorkingURL objects to compare proxy server results
    subscriber_queues = (csv_queue, proxy_queue) # For publisher coro to distribute good pages to tasks

    DOMAIN_FILENAME = "domains.txt"
    PAGEINFO_FILENAME = "page_info.csv"
    try:
        # Distribute deep copies of good pages to processing queues.
        loop.create_task(publisher(good_page_queue, subscriber_queues)) 
        # Build and publish URLs from domain list.  
        loop.create_task(make_urls(url_queue, DOMAIN_FILENAME)) 
        # Test URLs and publish "good" pages. 200 OK, with title, etc.
        loop.create_task(test_urls(url_queue, good_page_queue)) #
        # Write good URL data to CSV file.
        loop.create_task(write_page_info(csv_queue, PAGEINFO_FILENAME))
        # Compare good page to Forcepoint and Skyhigh proxy results
        loop.create_task(compare_proxy_servers(proxy_queue))
       
        # GO!!!!
        loop.run_forever()
    finally:
        # STOP!!!!
        loop.close()
        logging.info("Successfully shutdown.")

               
            
if __name__ == '__main__':    
    # Set up logging
    #LOG_LEVEL = logging.WARN
    LOG_LEVEL = logging.INFO
    #LOG_LEVEL = logging.DEBUG
    #LOG_LEVEL = logging.ERROR
    LOG_FMT_STR = "%(asctime)s,%(msecs)d %(levelname)s: %(message)s"
    LOG_DATEFMT_STR = "%H:%M:%S"

    logging.basicConfig(level=LOG_LEVEL, format=LOG_FMT_STR, datefmt=LOG_DATEFMT_STR)
    
    main()
