import json
from datetime import datetime
import os
import pandas as pd
from tqdm import tqdm

base_dir = "C:/Users/robert.franklin/Desktop/local_projects/random/marstons"

address_df = pd.DataFrame()
takeaway_hours_df = pd.DataFrame()
eatin_hours_df = pd.DataFrame()
category_item_links_df = pd.DataFrame()
items_df = pd.DataFrame()

files = os.listdir(f"{base_dir}/data/raw")
for i, f in tqdm(enumerate(files), total=len(files), desc="Processing files"):
    # Get the data from the json file
    data = json.load(open(f"{base_dir}/data/raw/{f}", "r"))
    # In the data the venue id is all but the last character of _id
    venue_id = data.get("_id", None)
    venue_name = data.get("name", None)
    # Address
    address = data.get("address", {})
    address["lng"], address["lat"] = address.get("location", {}) \
        .get("coordinates", [None, None])
    address["venue_id"] = venue_id
    address["name"] = data.get("name", None)
    # Covert to dataframe
    del address["_id"]
    del address["location"]
    address_df = pd.concat([
        address_df,
        pd.DataFrame(address, index=[i])
    ])
    # Opening hours - takeaway
    takeaway_hours = [
        {
            "venue_id": venue_id,
            "day": day_name,
            "is_open": day.get("open", False),
            "open": str(
                datetime(1, 1, 1,
                        hour=day.get("from", {}).get("hour", 0),
                        minute=day.get("from", {}).get("minute", 0)).time()
            ),
            "close": str(
                datetime(1, 1, 1,
                        hour=day.get("to", {}).get("hour", 0),
                        minute=day.get("to", {}).get("minute", 0)).time()
            ),
            "is_enabled": data.get("takeaway", {}).get("isEnabled", False),
            "min_lead_time": data.get("takeaway", {}).get("minLeadTime", None),
            "pick_up_time": data.get("takeaway", {}).get("pickUpTime", None)
        }
        for day_name, day in
        data.get("takeaway", {}).get("openingTimes", {}).items()
        if type(day) == dict
    ]
    # Covert to dataframe
    takeaway_hours_df = pd.concat([
        takeaway_hours_df,
        pd.DataFrame(takeaway_hours)
    ])
    # Opening hours - eat in
    eatin_hours = [
        {
            "venue_id": venue_id,
            "day": day_name,
            "is_open": day.get("open", False),
            "open": str(
                datetime(1, 1, 1,
                        hour=day.get("from", {}).get("hour", 0),
                        minute=day.get("from", {}).get("minute", 0)).time()
            ),
            "close": str(
                datetime(1, 1, 1,
                        hour=day.get("to", {}).get("hour", 0),
                        minute=day.get("to", {}).get("minute", 0)).time()
            ),
            "is_enabled": data.get("oat", {}).get("isEnabled", False)
        }
        for day_name, day in
        data.get("oat", {}).get("openingTimes", {}).items()
        if type(day) == dict
    ]
    # Covert to dataframe
    eatin_hours_df = pd.concat([
        eatin_hours_df,
        pd.DataFrame(eatin_hours)
    ])
    # Menu items
    menu = data.get("menus", {}) \
        .get("oat", {})
    category_item_links = [
        {
            "venue_id": venue_id,
            "category_id": cat.get("_id", None),
            "category_name": cat.get("name", None),
            "group_code": cat.get("groupCode", None),
            "set_code": cat.get("setCode", None),
            "item_id": item_id
        } 
        for cat in menu.get("categories", [])
        for item_id in cat.get("items", [])
        if cat.get("enabled", False) == True
        ]
    # Covert to dataframe
    category_item_links_df = pd.concat([
        category_item_links_df,
        pd.DataFrame(category_item_links)
    ])
    items = [
        {"item_id": id, "venue_id": venue_id} | item 
        for id, item in menu.get("items", {}).items()
        ]
    # Covert to dataframe
    items_df = pd.concat([
        items_df,
        pd.DataFrame(items)
    ])


# Export to parquet
address_df = address_df.to_parquet(f"{base_dir}/data/processed/address.parquet")
takeaway_hours_df = takeaway_hours_df.to_parquet(f"{base_dir}/data/processed/takeaway_hours.parquet")
eatin_hours_df = eatin_hours_df.to_parquet(f"{base_dir}/data/processed/eatin_hours.parquet")
category_item_links_df = category_item_links_df.to_parquet(f"{base_dir}/data/processed/category_item_links.parquet")
items_df = items_df.to_parquet(f"{base_dir}/data/processed/items.parquet")