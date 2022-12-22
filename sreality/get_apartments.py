# 
from joblib import Parallel
from joblib import delayed
import requests
import pickle
import shutil
import json
import time
import os


API_HEADER = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0" ,
    "Accept-Encoding": "gzip, deflate, br", "Accept-Language": "en-US,en;q=0.5",
    "Upgrade-Insecure-Requests": "1", "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate", "Connection": "keep-alive",
    "Host": "www.sreality.cz", "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1", "TE": "trailers",
}


def property_fetch(id):
    """Fetches property profile from sreality"""
    try: 
        url = f"https://www.sreality.cz/api/cs/v2/estates/{id}"
        res = requests.get(url, headers=API_HEADER).json()
        res["id"] = id
        return res
    except: return id


def property_fetch_write(id, idx):
    """Fetches and dumps property profile from sreality"""
    property = property_fetch(id)
    if isinstance(property, dict):
        with open(PROFILES_PATH + f"{id}.json", "w") as f:
            json.dump(property, f); time.sleep(0.5)
            print(f"Successful {idx}: {id}")
    else: return id


if __name__ == "__main__":

    # set urls and params
    SEARCH_RESULT_LINK = "https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_type_cb=1"
    SEARCH_RESULT_LINK = SEARCH_RESULT_LINK + "&locality_region_id=10&no_auction=1&no_shares=1&"
    SEARCH_RESULT_LINK = SEARCH_RESULT_LINK + "per_page=60&page="
    NUM_PAGES = 99

    # make local dirs for dumping
    LIST_PATH = "./sreality/.data/.list/"
    if os.path.isdir (LIST_PATH): 
        shutil.rmtree(LIST_PATH)
    os.makedirs(LIST_PATH)

    # scrape the list of aparment urls from search results
    for page in range(2, NUM_PAGES):
        url = SEARCH_RESULT_LINK + str(page)
        res = requests.get(url).json().get("_embedded").get("estates")
        res = [r.get("_links").get("self").get("href") for r in res]
        with open(LIST_PATH + f"{page}.pkl", "wb") as f: 
            pickle.dump(res, f)
            time.sleep(0.1)
            print(page)

    # make local dirs for dumping
    PROFILES_PATH = "./sreality/.data/.profiles/"
    if os.path.isdir (PROFILES_PATH): 
        shutil.rmtree(PROFILES_PATH)
    os.makedirs(PROFILES_PATH)
    
    # concat apartment lists from search result pages
    properties = []
    for f in os.listdir(LIST_PATH):
        with open(LIST_PATH + f, "rb") as f:
            properties = properties + [
                p.split("/")[-1].split("?")[0] 
                for p in pickle.load(f)
            ]
    
    # scrape and dump individual apartment data
    failed = Parallel(n_jobs=8, backend="threading")(
        delayed(property_fetch_write)(id, idx) 
        for idx, id in enumerate(properties)
    )
