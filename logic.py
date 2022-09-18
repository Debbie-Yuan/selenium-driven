# Open a new Firefox browser
# Load the page at the given URL

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from statics import CHROME_WEB_DRIVER_PATH


# 是否被Cloudflare等拦截，拦截信息过滤器，传入整个网页内容，请求，响应，通过提示信息处理该内容


browser = webdriver.Chrome(service=Service(executable_path=CHROME_WEB_DRIVER_PATH))
browser.get("https://hotlink.cc/YHHHBBK9NGFG/XMG20.html")