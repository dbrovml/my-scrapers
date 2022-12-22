from joblib import Parallel
from joblib import delayed
import numpy as np
import lxml.html
import requests
import pickle
import shutil
import time
import json
import os


def get_work(work):
    """Scrapes and dumps work metadata and image."""

    # parse work detail page url
    url = f"https://www.wikiart.org/en/{work}"

    # fetch work detail page content
    while True:
        try:
            res = requests.get(url)
            xml = lxml.html.fromstring(res.content)
            break
        except:
            times = [0.1, 0.5, 1.0]; probs = [0.80, 0.15, 0.05]
            time.sleep(np.random.choice(times, p=probs))
    
    # get work image from source endpoint
    try:
        img = xml.xpath("/html/body/div[2]/div[1]/section[1]/main/div[2]/aside/div[1]")
        img = img[0].xpath(".//img[@itemprop='image']")
        img = img[0].attrib.get("src")

        # dump image locally
        with open(f"./wikiarts/.data/images/{work.replace('/', '---')}.jpg", "wb") as f:
            image = requests.get(img, stream=True).raw
            shutil.copyfileobj(image, f)
    except:
        return work
    
    # extract basic information (style, genre, etc.)
    try:
        inf = xml.xpath("/html/body/div[2]/div[1]/section[1]/main/div[2]/article")
        inf = inf[0].xpath(".//*/li[@class='dictionary-values ']")
        inf = {
            e.find("s").text_content().replace(":", ""): e.xpath("span")[0]
            .text_content() .replace("\n", "").strip() for e in inf
        }
    except:
        inf = {}

    # extract tags related to the work (nature, human, cafe, etc.)
    try:
        tag = xml.xpath(".//*/div[@class='tags-cheaps__item']")
        tag = [e.find("a").text_content() for e in tag]
        tag = [e.replace("\n", "").strip() for e in tag]
    except:
        tag = []

    # dump metadata locally
    with open(f"./wikiarts/.data/metadata/{work.replace('/', '---')}.json", "w") as f:
        json.dump({"url": url, "img": img, **inf, "tag": tag}, f)


if __name__ == "__main__":

    # make local dirs for dumping
    if not os.path.isdir("./wikiart/.data/metadata/"):
        os.makedirs("./wikiart/.data/metadata/")

    if not os.path.isdir("./wikiart/.data/images/"):
        os.makedirs("./wikiart/.data/images/")

    # load the list of works
    list_path = "./wikiarts/.data/works_list.pkl"
    with open(list_path, "rb") as f: works = pickle.load(f)
    
    # scrape works, return failed works
    failed = Parallel(n_jobs=8, backend="threading")(
        delayed(get_work)(work) for work in works)
    
    # try the scrape once more for failed works
    _ = Parallel(n_jobs=8, backend="threading")(
        delayed(get_work)(work) for work in failed)
