#
import shutil
import json
import os

import pandas as pd
import numpy as np

from selenium import webdriver
from bs4 import BeautifulSoup

import requests
import time
import re

from joblib import Parallel, delayed


cHomepage = "https://www.rohlik.cz/"

def makeDataFolders():
    dataFolders = ["./data/itemDetail", "./data/itemImage", "./data/itemTrees"]
    for dataFolder in dataFolders:
        if not os.path.isdir(dataFolder):
            os.mkdir(dataFolder)

def getParentCategoryList():
    """Get the list of 1st level categories from the homepage."""
    res = requests.get(cHomepage)
    res = BeautifulSoup(res.content, "html.parser")
    res = res.findAll("li", {"class": "sortimentItem"})
    parentCategoryList = [cat.find("a")["href"] for cat in res]
    parentCategoryList = [cat for cat in parentCategoryList if "lekarna" not in cat]
    return parentCategoryList

def getChildCategoryList(parentCategory):
    """Get the list of child categories from a parent category page."""
    res = requests.get(cHomepage[:-1] + parentCategory)
    res = BeautifulSoup(res.content, "html.parser")
    res = res.findAll("a", {"class": "category_link"})
    childCategoryList = [cat["href"] for cat in res]
    return childCategoryList

def getCategoryLists():
    """Get the lists of parent and child categories."""
    parentCategoryList = getParentCategoryList()
    child1CategoryList = [
        getChildCategoryList(parentCategory) 
        for parentCategory in parentCategoryList
    ]
    child2CategoryList = [
        [getChildCategoryList(parentCategory) 
        for parentCategory in parentCategoryList] 
        for parentCategoryList in child1CategoryList
    ]
    return (
        parentCategoryList, child1CategoryList, child2CategoryList
    )

def buildCategoryTree():
    """Parse category lists into a hierarchical tree."""
    (
        parentCategoryList, child1CategoryList, child2CategoryList
    ) = getCategoryLists()
    parentCategoryListExploded = []
    child1CategoryListExploded = []
    child2CategoryListExploded = []
    for parIdx, parCat in enumerate(parentCategoryList):
        for ch1Idx, ch1Cat in enumerate(child1CategoryList[parIdx]):
            ch2CatList = child2CategoryList[parIdx][ch1Idx]
            if len(ch2CatList) != 0:
                for ch2Cat in ch2CatList:
                    parentCategoryListExploded.append(parCat)
                    child1CategoryListExploded.append(ch1Cat)
                    child2CategoryListExploded.append(ch2Cat)
            else:
                parentCategoryListExploded.append(parCat)
                child1CategoryListExploded.append(ch1Cat)
                child2CategoryListExploded.append(ch1Cat)

    return pd.DataFrame({
        "CAT0": parentCategoryListExploded,
        "CAT1": child1CategoryListExploded,
        "CAT2": child2CategoryListExploded
    })

def getChildItemList(parentCategory):
    """Get the list of child items from a parent category page."""
    driver.get(cHomepage[:-1] + parentCategory); time.sleep(3)
    itemList = driver.find_elements_by_xpath("//article")
    itemList = [item.get_attribute("data-productid") for item in itemList]
    itemList = [item for item in itemList if item is not None]
    return itemList

def getItemList():
    """Get the lists of child items."""
    parentCategoryList = categoryTree.CAT2.tolist()
    itemList = []
    for idx, parentCategory in enumerate(parentCategoryList):
        try:
            itemList.append(getChildItemList(parentCategory))
            print(f"CAT num. {idx} / {len(parentCategoryList)}.")
        except:
            itemList.append([])
            print(f"CAT num. {idx} FAILED.")
    return itemList

def buildItemTree():
    """Parse the item list into a hierarchical tree."""
    parentCategoryList = categoryTree.CAT2.tolist()
    itemList = getItemList()
    itemListExploded = []
    parentCategoryListExploded = []
    for parIdx, parCat in enumerate(parentCategoryList):
        if len(itemList[parIdx]) != 0:
            for item in itemList[parIdx]:
                parentCategoryListExploded.append(parCat)
                itemListExploded.append(item)
        else:
            pass
    itemTree = pd.DataFrame({
        "CAT2": parentCategoryListExploded,
        "ITEM": itemListExploded
    })
    itemTree = itemTree.groupby("ITEM", as_index=False).first()
    return itemTree

def getUpperLower(item):
    """Get upper and lower grids of a product detail page."""
    res = requests.get(cHomepage + str(item))
    res = BeautifulSoup(res.content, "html.parser")
    res = res.find("div", {"class": "sc-6mbxxv-3 iDgOEw"})
    res = res.find("div", {"class": "o6rnl5-0 jaOFxy"})
    res = res.find("div", {"class": "sc-3gouww-0 bKWYmo"})
    return (
        res.find("div", {"class": "sc-3gouww-1 eKCBTO"}),
        res.find("div", {"class": "sc-1r0dgdh-0 hgMzsO"})
    )

def saveItemImage(upper, item):
    """Parses an item image url, fetches and dumps the image."""
    try:
        imageUrl = (
            upper.find("div", {"class": "sc-3gouww-4 jngAXY"})
            .find("div", {"class": "ik84rk-0 hyRGQA"})
            .find("div", {"class": "ik84rk-1 cZLxXu"})
            .find("img").get("src").split("https://")[-1]
        )
        image = requests.get("https://" + imageUrl, stream=True)
        with open(f"./data/itemImage/{item}.png", "wb") as imfile:
            shutil.copyfileobj(image.raw, imfile)
    except:
        pass

def getItemTitle(upper):
    """Get item title."""
    try:
        return (
            upper.find("div", {"class": "sc-3gouww-5 deGZAB"})
            .find("h2", {"class": "sc-3gouww-6 iAWHmR"})
            .a.text
        )
    except:
        return "<NA>"

def getItemQuant(upper):
    """Get item quantity."""
    try:
        return (
            upper.find("div", {"class": "sc-3gouww-5 deGZAB"})
            .span.text
        )
    except:
        return "<NA>"

def getItemPrice(upper):
    """Get item price."""
    try:
        return (
            upper.find("div", {"class": "sc-3gouww-5 deGZAB"})
            .find("div", {"class": "sc-1n5zgx7-0 QZkVI"})
            .find("span", {"class": "unitPrice"})
            .text
        )
    except:
        return "<NA>"

def getItemTexts(lower):
    """Get item text fields - description, storage info, etc."""
    try:
        return (
            lower.find("div", {"class": "tabsContent react-tabs__tab-panel--selected"})
            .find("div", {"class": "sc-10utft9-0 byxHVw"})
            .find("div", {"class": "sc-10utft9-1 iEsFUX"})
            .find("div", {"class": "sc-10utft9-2 dnAjXw"})
            .find("div", {"class": "ckContent"})
            .text
        )
    except:
        return "<NA>"

def getItemTable(lower):
    """Get item table of content, nutrition values and list of allergenes."""
    itemCompCols = (
        lower.find("div", {"class": "tabsContent react-tabs__tab-panel--selected"})
        .find("div", {"class": "sc-10utft9-0 byxHVw"})
        .find("div", {"class": "cwr3a8-0 dqBEww"})
        .findAll("div", {"class": "composition_col"})
    )
    itemCompColsDict = {}
    for itemCompCol in itemCompCols:
        itemCompColName = itemCompCol.h3.text
        itemCompColRows = itemCompCol.table.tbody.findAll("tr")
        if (itemCompColName == "Složení"):
            itemCompColCont = []
            for itemCompColRow in itemCompColRows:
                try:
                    itemCompColCont.append(
                        itemCompColRow.find("td", {"class": "level_0"})
                        .span.text
                    )
                except:
                    pass
            itemCompColsDict.update({itemCompColName: itemCompColCont})
        elif (itemCompColName == "Nutriční hodnoty na 100 g"):
            itemCompColCont = {}
            for itemCompColRow in itemCompColRows:
                try:
                    key = itemCompColRow.findAll("td")[0].text
                    val = itemCompColRow.findAll("td")[1].text
                    itemCompColCont.update({key: val})
                except:
                    pass
            itemCompColsDict.update({itemCompColName: itemCompColCont})
        elif (itemCompColName == "Alergeny"):
            itemCompColCont = []
            for itemCompColRow in itemCompColRows:
                try:
                    itemCompColCont.append(itemCompColRow.td.text)
                except:
                    pass
            itemCompColsDict.update({itemCompColName: itemCompColCont})
        else:
            pass
    return itemCompColsDict

def saveItemDetail(upper, lower, item):
    """Get item details, parse as dict, dump as json."""
    itemDetail = {
        "itemQuant": getItemQuant(upper), "itemPrice": getItemPrice(upper),
        "itemTitle": getItemTitle(upper), "itemTexts": getItemTexts(lower),
        "itemTable": getItemTable(lower)
    }
    with open(f"./data/itemDetail/{item}.json", "w") as jsonfile:
        json.dump(itemDetail, jsonfile)

def getItem(item):
    """Gets and dumps image and details for a given item."""
    time.sleep(0.01)
    try:
        upper, lower = getUpperLower(item)
        saveItemDetail(upper, lower, item)
        saveItemImage(upper, item)
    except:
        pass

def main():

    makeDataFolders()
    driver = webdriver.Firefox(executable_path="./driver/geckodriver.exe")

    categoryTree = buildCategoryTree(); itemTree = buildItemTree()
    categoryTree.to_csv("./data/itemTrees/categoryTree", index=None)
    itemTree.to_csv("./data/itemTrees/itemTree", index=None)

    _ = Parallel(n_jobs=128, backend="threading")(
        delayed(getItem)(item) for item in itemTree.ITEM
    )

    successItem = os.listdir("./data/itemDetail/")
    successItem = [int(i.split(".")[0]) for i in successItem]
    failureItem = [i for i in itemTree.ITEM if i not in successItem]

    _ = Parallel(n_jobs=128, backend="threading")(
        delayed(getItem)(item) for item in failureItem
    )

if __name__ == "__main__":
    main()
