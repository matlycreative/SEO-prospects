import os

BOT_NAME = "linkedin"

SPIDER_MODULES = ["linkedin.spiders"]
NEWSPIDER_MODULE = "linkedin.spiders"

ROBOTSTXT_OBEY = False

USER_AGENT = "Mozilla/5.0"
LOG_LEVEL = "INFO"

# ---- ScrapeOps (only enable if key is present) ----
SCRAPEOPS_API_KEY = os.getenv("SCRAPEOPS_API_KEY", "")
SCRAPEOPS_PROXY_ENABLED = bool(SCRAPEOPS_API_KEY)

if SCRAPEOPS_PROXY_ENABLED:
    EXTENSIONS = {
        "scrapeops_scrapy.extension.ScrapeOpsMonitor": 500,
    }

    DOWNLOADER_MIDDLEWARES = {
        "scrapeops_scrapy.middleware.retry.RetryMiddleware": 550,
        "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
        "scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk": 725,
    }

    CONCURRENT_REQUESTS = 1
