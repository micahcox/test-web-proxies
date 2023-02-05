import asyncio
import aiohttp
import aiofiles
import aiologger
import uuid

LOG_FMT_STR = "%(asctime)s,%(msecs)d %(levelname)s: %(message)s"
LOG_DATEFMT_STR = "%H:%M:%S"
aio_formatter = aiologger.formatters.base.Formatter(
    fmt=LOG_FMT_STR, datefmt=LOG_DATEFMT_STR,
)
logger = aiologger.Logger.with_default_handlers(formatter=aio_formatter)
