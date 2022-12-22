from joblib import Parallel
from joblib import delayed
import lxml.html
import requests
import string
import pickle
import time
import os


def get_artist_list():
    """Scrapes the list of artists presented at wikiart."""
    artist_list = []
    for letter in list(string.ascii_lowercase):
        url = f"https://www.wikiart.org/en/Alphabet/{letter}/text-list"
        xml = lxml.html.fromstring(requests.get(url).content)
        out = xml.xpath("/html/body/div/div[1]/section/main/div[2]/ul")
        out = [e.attrib.get("href") for e in out[0].xpath(".//a")]
        out = [e.replace("/en/", "") for e in out]
        artist_list += out
    return artist_list


def get_artist_works(artist):
    """Scrapes the list of works of each artist."""
    url = f"https://www.wikiart.org/en/{artist}/all-works/text-list"
    while True:
        try:
            res = requests.get(url)
            if res.status_code == 200:
                xml = lxml.html.fromstring(res.content)
                out = xml.xpath("/html/body/div/div[1]/section/main/div[2]/ul")
                out = [e.attrib.get("href") for e in out[0].xpath(".//*/a")]
                out = [e.replace("/en/", "") for e in out]
                return out
        except:
            time.sleep(0.05)


if __name__ == "__main__":

    # make local dirs for dumping
    if not os.path.isdir("./wikiart/.data/"):
        os.makedirs("./wikiart/.data/")

    # get the list of artists
    artist_list = get_artist_list()

    # get the list of works for each artist
    artist_works = Parallel(n_jobs=8, backend="threading")(
        delayed(get_artist_works)(artist) for 
        artist in artist_list)

    # drop empty work lists returned for some artists
    valid_artist_works = []
    for artist_works_list in artist_works:
        if len(artist_works_list) != 0:
            valid_artist_works += artist_works_list
    
    # drop corrupted work names
    valid_artist_works = [
        work for work in valid_artist_works 
        if "http" not in work]
    
    # dump work list locally
    list_path = "./wikiarts/.data/works_list.pkl"
    with open(list_path, "wb") as f: pickle.dump(valid_artist_works, f)
