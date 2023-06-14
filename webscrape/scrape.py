from async_scrape import Scrape
import requests
import json

from selenium.webdriver import Edge
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url = "https://order.marstons.co.uk/"
base_dir = "C:/Users/robert.franklin/Desktop/local_projects/random/marstons"

# GET ALL RESTAURANT DATA - selenium
browser = Edge()
browser.get(url)
wait = WebDriverWait(browser, 100).until(
    EC.presence_of_all_elements_located((By.CLASS_NAME, "venues-list"))
)
elements = browser.find_elements(By.CLASS_NAME, "venue-card")
hrefs = [e.get_dom_attribute("href") for e in elements]
browser.close()
print(f"Fetched {len(hrefs)} hrefs from {url}")

def post_process_func(html, resp, *args, **kwargs):
    # Save to file
    fn = resp.url.split("/")[-1]
    content = json.loads(resp.content)
    with open(f"{base_dir}/data/raw/{fn}.json", "w") as f:
        json.dump(content, f, indent=4)

base_url = "https://api-cdn.orderbee.co.uk/venues"
urls = [base_url + href for href in hrefs]
scrape = Scrape(post_process_func=post_process_func)
print(f"Begin scrape of {len(urls)} - Example: {urls[0]}")
scrape.scrape_all(urls)