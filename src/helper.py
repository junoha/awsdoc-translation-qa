from datetime import datetime
import logging
import time
import traceback

import lxml
from lxml.html.clean import clean_html

logger = logging.getLogger("crawler").getChild(__name__)


def calc_time(fn):
    """
    Decorator that measures execution time of function
    """

    def wrapper(*args, **kwargs):
        start = time.time()
        fn(*args, **kwargs)
        end = time.time()
        logger.info(f"[{fn.__name__}] elapsed time: {end - start}")
        return

    return wrapper


def url_to_path(url: str):
    """
    Convert URL to path
    """
    return url.replace("://", "___").replace(".", "__").replace("/", "_")


def path_to_url(path: str):
    """
    Convert path to URL
    """
    return path.replace("___", "://").replace("__", ".").replace("_", "/")


def chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def to_isoformat(date_str: str) -> str:
    """
    Last-modified to isoformat
    Sat, 27 Jun 2020 02:00:18 GMT => 2020-06-27T02:00:18
    """
    return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").isoformat()


def is_ok_url(url: str):
    """
    Check doc url is valid
    """
    ng_list = [
        "aws-sdk-php",
        "AWSAndroidSDK",
        "AWSiOSSDK",
        "AWSJavaScriptSDK",
        "AWSJavaSDK",
        "awssdkrubyrecord",
        "encryption-sdk",
        "mobile-sdk",
        "pythonsdk",
        "powershell",
        "sdk-for-android",
        "sdk-for-cpp",
        "sdk-for-go",
        "sdk-for-ios",
        "sdk-for-java",
        "sdk-for-javascript",
        "sdk-for-net",
        "sdk-for-php",
        "sdk-for-php1",
        "sdk-for-ruby",
        "sdk-for-unity",
        "sdkfornet",
        "sdkfornet1",
        "xray-sdk-for-java",
        "code-samples",
    ]

    for ng in ng_list:
        if ng in url:
            return False

    return True


def filter_data(data_list):
    """
    Filter and normalize AWS document data
    """
    doc_list = []

    for data in data_list:
        url = data["url"]
        logger.info("  Start {}".format(url))

        # Filter by URL
        if data["status"] != 200:
            logger.warning("No 200")
            continue
        if "apireference" in url.lower():
            logger.warning("API Reference")
            continue
        if "/cli/" in url.lower():
            logger.warning("AWS CLI")
            continue
        if "/code-samples/" in url.lower():
            logger.warning("code samples")
            continue

        try:
            h = lxml.html.fromstring(data["html"])
            title = h.cssselect("title")[0].text

            product = None
            guide = None
            try:
                for meta in h.cssselect("meta"):
                    if meta.get("name") == "product":
                        product = meta.get("content")
                    if meta.get("name") == "guide":
                        guide = meta.get("content")
            except Exception:
                pass

            # Normalize by clean_html
            # https://lxml.de/lxmlhtml.html#cleaning-up-html
            content = clean_html(h).text_content()
            content = "".join([line.strip()
                               for line in content.splitlines()])

            doc_list.append(
                {
                    "url": url,
                    "product": product,
                    "guide": guide,
                    "title": title,
                    "content": content,
                    "raw_html": data["html"],
                    "last_modified": data["last_modified"],
                    "crawled_at": data["crawled_at"],
                }
            )

        except Exception:
            logger.exception(traceback.format_exc())
            logger.warning("  Skipping this URL... {}".format(url))
            continue

    return doc_list
