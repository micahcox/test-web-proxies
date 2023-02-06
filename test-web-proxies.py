import aiohttp
import asyncio
import aiofiles
import aiologger
import logging
import signal
import uuid
import re

# Exception handling for asyncio loop.
def handle_exception(loop, context):
    msg = context.get("exception", context["message"])
    logging.error(f"Caught exception: {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(loop))
    
async def shutdown(loop, signal = None):
    if signal:
        await logger.info(f"Received exit signal {signal.name}...")
    
    # Make a list of tasks, not including this shutdown task.
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    
    # Cancel all tasks.
    [task.cancel() for task in tasks]
    
    await logger.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    await logger.info(f"Shutting down aiologger")
    await logger.shutdown()
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


async def get_page(url: WorkingURL, session: aiohttp.ClientSession, return_body = False)-> WorkingURL:
    
    TITLE_RE = re.compile(r'<title>(.*?)</title>', flags = re.IGNORECASE)
    
    try:
            async with session.get(url.url) as response:
                
                url.response: aiohttp.ClientResponse = response
                url.response_text = None
               
                if response.status == 200:
                    logger.debug(f"{url.url_id} - {response.status} OK - {response.url}")
                else:
                    await logger.debug(f"{url.url_id} - response code {response.status} - {response.url}")
                    
                    
                if response.status == 403:  # Forbidden
                    #TODO: Common websites return this.  Find out why and respond accordingly.  Could be SSL or other issue.
                    await logger.error(f"{url.url_id} - {response.status} - {response.reason} - {response.url}")
                    response.close()
                    return url
                
                if response.status >= 400:
                    await logger.error(f"{url.url_id} - {response.status} - {response.reason} - {response.url}")
                    response.close()
                    return url
                
                if return_body:
                    if response.status == 200:
                        await logger.debug(f"{url.url_id} - getting html page from URL - {response.url}")
                        url.response_text = await response.text()
                        # Get page title
                        if title :=  TITLE_RE.search(url.response_text): # if not Null
                            url.title = title.group(1) # then assign what's between title tags
                            await logger.debug(f"{url.url_id} - got page title - {url.title} for url: {response.url}")
                        else:
                            url.title = None # Title not found
                            await logger.debug(f"{url.url_id} - Title not foudn in page for url: {response.url}") 
                    else:
                        await logger.debug(f"{url.url_id} - response status {response.status} so not attempting to retrieve html page from URL - {response.url}")        
                # Explicitly close the session    
                response.close()
            return url
        
    except aiohttp.ClientConnectionError as err:
        await logger.info(f"{url.url_id} - Connection error - {url.domain} - {url.url} - {err}")
        return url

    except asyncio.CancelledError as err:     
        await logger.info(f"{url.url_id} - Get page cancelled - {url.domain} - {url.url} - {err}")
        response.close()
        return url
    
    except Exception as err:
        await logger.info(f"{url.url_id} - get_page unknown Exception - {url.domain} - {url.url} - {err}")
        return url


# Queues a series of URL objects built from a list of domains, for later processing.       
async def make_urls(queue, filename: str):

    async with aiofiles.open(filename, mode = "r") as domains:
        async for raw_domain in domains:
            domain = raw_domain.strip().lower()
            await logger.debug(f"Queuing URLs for domain {domain}") 

            # queue an async task for various combinations of domain and protocol
            for url in make_url_list(domain):
                url_id = str(uuid.uuid4()) # UUID for each URL
                url = WorkingURL(url_id = url_id, domain = domain, url = url)
                asyncio.create_task(queue.put(url))
                await logger.debug(f"{url.url_id} for {url.url} queued.")


async def test_urls(queue):
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
            url = await queue.get()
            await logger.info(f"Processing URls for domain {url.domain}")
            asyncio.create_task(get_page(url, session, return_body = True))

               
def main():

    # AsyncIO event loop setup
    
    #DEPRECATED
    #loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    
    # Signal handler for more graceful shutdown at any point during runtime.
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            # signal=signal fixes late binding with lambda.  Google it.
            s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
        )
    loop.set_exception_handler(handle_exception)
    queue = asyncio.Queue()

    DOMAIN_FILENAME = "domains.txt"
    try:
        loop.create_task(make_urls(queue, DOMAIN_FILENAME)) # publish
        loop.create_task(test_urls(queue)) # consume. Find working URLs
        # TODO: Write good URLs for consumption instead of generating every time.
        # TODO: good URLs can be tested against multiple proxy servers here.
        # GO!!!!
        loop.run_forever()
    finally:
        # STOP!!!!
        loop.close()
        logging.info("Successfully shutdown.")
        
        
if __name__ == '__main__':
    # Logging formats
    LOG_FMT_STR = "%(asctime)s,%(msecs)d %(levelname)s: %(message)s"
    LOG_DATEFMT_STR = "%H:%M:%S"
    
    # Setup non-async function logging
    logging.basicConfig(level=logging.DEBUG, format=LOG_FMT_STR, datefmt=LOG_DATEFMT_STR)
    logging.getLogger("aiohttp").setLevel(logging.DEBUG)
    
    # Setup async logger
    aio_formatter = aiologger.formatters.base.Formatter(
        fmt=LOG_FMT_STR, datefmt=LOG_DATEFMT_STR,
    )
    logger = aiologger.Logger.with_default_handlers(formatter=aio_formatter)
    
    main()
