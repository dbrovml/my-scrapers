#
from unidecode import unidecode
from joblib import Parallel
from joblib import delayed
from PIL import Image
import pandas as pd
import numpy as np
import imghdr
import json
import os


def check_image(image):
    """Checks if a scraped image is a valid image."""
    try:
        path = "./wikiarts/.data/images/" + image
        if np.array(Image.open(path)).shape[-1] != 3:
            return image
        if imghdr.what(path) != "jpeg":
            return image
    except: return image


def load_metadata(file):
    """Load work metadata json, add work name and author items."""

    imagefile   = file.split( "/" )[-1].replace(".json", ".jpg")
    work_name   = file.split("---")[1 ].replace(".json", "")
    work_author = file.split("---")[0 ]

    year  = None
    parts = work_name.split("-")
    parts.reverse()
    for part in parts:
        if len(part) == 4:
            try   : year = int(part)
            except: pass

    with open(metadata_dir + file, "rb") as f:
        metadata = json.load(f)

    metadata.update({"author": work_author})
    metadata.update({"name"  : work_name  })
    metadata.update({"imagef": imagefile  })
    metadata.update({"year"  : year       })

    return metadata


if __name__ == "__main__":

    # load work metadata
    metadata_dir = "./wikiarts/.data/metadata/"
    metadata_dicts = Parallel(n_jobs=32, backend="threading")(
        delayed(load_metadata)(file) for file 
        in os.listdir(metadata_dir))
    index = pd.DataFrame(metadata_dicts)

    # prettify genre, style, media
    for col in ["Style", "Genre", "Media"]:
        func = lambda col: [s.strip() for s in col.split(",")]
        index.loc[index[col].isna(),  col ] = "<NA>"
        index[col] = index[col].apply(func)
        func = lambda col: unidecode(" | ".join(col ).lower())
        index[col] = index[col].apply(func)
    
    # prettify tags
    func = lambda col: unidecode(" | ".join(col ).lower())
    index["tag"] = index["tag"].apply(func)
    
    # find and drop corrupted images
    corrupted = Parallel(n_jobs=32, backend="threading")(
        delayed(check_image)(image) for image
        in index.imagef)

    corrupted = [image for image in corrupted if image]
    index = index.loc[~index.imagef.isin(corrupted), :]
    for image in corrupted: os.remove(corrupted)

    # dump works index locally
    index.to_csv("./wikiarts/.data/index.csv", index=None)
