#
import pandas as pd
import json
import os


def parseItemMetadata(path):
    """Load and parse item metadata json file."""
    itemId = path.split("/")[-1] .split(".")[0]
    with open(path, "r") as f:
        itemMetadata = json.load(f)
        itemMetadata.update({"ITEM": itemId})
    return itemMetadata

itemMetadataPaths    = [f"./data/metadata/{f}" for f in os.listdir("./data/metadata")]
itemMetadataDicts    = [parseItemMetadata(path) for path in itemMetadataPaths]
itemMetadata         = pd.DataFrame(itemMetadataDicts)
itemMetadata["ITEM"] = itemMetadata["ITEM"].astype(int)

treeCat  = pd.read_csv("./data/trees/categoryTree")
treeItem = pd.read_csv("./data/trees/itemTree")

itemMetadata = itemMetadata.merge(treeItem, on="ITEM")
itemMetadata = itemMetadata.merge(treeCat , on="CAT2", how="left")
itemImageIds = [path.split(".")[0] for path in os.listdir("./data/images")]
itemImageIds = [int(imageId) for imageId in itemImageIds]
itemImageIds = pd.DataFrame({"ITEM": itemImageIds})

itemMetadata = itemMetadata.merge(itemImageIds, on="ITEM", how="inner")
itemMetadata.to_csv("./data/indexedItems.csv", index=None)
