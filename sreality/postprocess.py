#
from unidecode import unidecode
import pandas as pd
import json
import os


# define fields of interest, numeric and categorical attributes
ITEM_CAT_NAMES = ["price_czk", "string", "boolean", "energy_efficiency_rating"]
ITEM_NUM_NAMES = ["count", "area"]
ITEM_SET_NAMES = ["set"]
SELECTED_FACTS = [
    "price", "place", "name", "Stavba", "Stav objektu", "Vlastnictví", "Podlaží",
    "Energetická náročnost budovy", "Výtah", "Užitná plocha", "Umístění objektu", 
    "Vybavení", "Parkování", "Terasa", "Balkón", "Lodžie", "Sklep",
    "Bezbariérový", "Garáž", "Bazén", "id", "locality_district_id",
    "Voda", "Topení", "Odpad", "Doprava", "Komunikace"
]


def property_parse(property):
    """Extracts detail attributes of a property."""

    cat_facts = {}
    num_facts = {}
    set_facts = {}

    # extract numeric and categorical attributes
    for item in property.get("items"):
        if item.get("type") in ITEM_NUM_NAMES:
            num_facts.update({item.get("name"): float(item.get("value"))})
        if item.get("type") in ITEM_CAT_NAMES:
            cat_facts.update({item.get("name"): str  (item.get("value"))})
        if item.get("type") in ITEM_SET_NAMES:
            set_facts.update(
                {
                    item.get("name"): " | ".join(
                        [ str(e.get("value")) for e in item.get("value")]
                    )
                }
            )
    
    parsed = {
        "price": property.get("price_czk").get("value"),
        "place": property.get("locality").get("value"), 
        "name" : property.get("name").get("value"),
        "id"   : property.get("id"), 
        **cat_facts, **num_facts,
        **set_facts
    }
    
    return {**{k: None for k in SELECTED_FACTS if k not in parsed.keys()}, **parsed}


def property_unify(property):
    """Converts json response to pd df."""
    if not isinstance(property, list): property = [property]
    property = pd.DataFrame(property).loc[:, SELECTED_FACTS]
    columns  = [c.replace(" ", "_" ) for c in property.columns]
    property.columns = [unidecode(c.lower()) for c in columns ]
    return property


if __name__ == "__main__":

    # read and parse raw json apartment profiles
    properties = []
    for id in os.listdir("./sreality/.data/.profiles/"):
        with open(f"./sreality/.data/.profiles/{id}", "r") as f:
            try:
                property = property_parse(json.load(f))
                properties.append(property)
            except:
                print(id)

    # collect apartment profiles into a single df and dump
    properties = property_unify(properties)
    properties.to_csv("./sreality/.data/processed.csv", index=None)

import pandas as pd
foo = pd.read_csv("./sreality/.data/processed.csv")
foo.shape