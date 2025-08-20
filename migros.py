import os
import time
from curl_cffi import requests
from datetime import datetime
import json
import pydash as _
import copy
import re
from DrissionPage import Chromium
from concurrent.futures import ThreadPoolExecutor
import threading


headers_lock1 = threading.Lock()

headers_lock2 = threading.Lock()

got_pro_id_list = []
ids_list = []

# token = "f42a5b59aec3467e97a8794c611c436b91589634343"
# proxyModeUrl = "http://{}:@proxy.scrape.do:8080".format(token)
# proxies = {
#     "http": proxyModeUrl,
#     "https": proxyModeUrl,
# }


global_headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'leshopch': 'eyJsdmwiOiJVIiwiZW5jIjoiQTI1NkdDTSIsImFsZyI6ImRpciIsImtpZCI6ImZiYzk1OGFmLTlkNmQtNDdhZC1hZmFiLTI5ZTJhZTYxNzA3ZSJ9..6Sf7LNuEkaaAiBIG.AEJ_D44TaBw3ZEIk9n6md3vbzt574GNkc8GkycmQ1LpYdOmLwvKLE5YHc1MvaM8FUOV8UZQuDWTPW39kGczP1wfnztfmhh7nrAmp__txKtrxhwrfVrzSZj43IdefVEOQy07AsbxzWzO76qkf67pgP4rwZNfvqI29yHT6aAO4rQqmFSbqXgnNuV0C5EN7udMo4HlwGsjtkZ1ud7DC5lDWWv1Snk-kwg9YLBiZadlcMEM-XyWshtM4hgxui4yMI72cxwGwtrXCQwlKyOngLBJ4jBugYy8p.4AVPKyDD40PcE1WlmsdiCg',
    'migros-language': 'en',
    'origin': 'https://www.migros.ch',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.migros.ch/',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
}

def get_header():
    browser = initialize_browser()
    tab = browser.latest_tab

    tab.listen.start('https://www.migros.ch/authentication/public/v1/api/guest?authorizationNotRequired=true')
    tab.get('https://www.migros.ch/en')
    res = tab.listen.wait()
    # print(res.response.body)
    tab.listen.stop()
    leshopch = res.response.headers
    # print(leshopch['leshopch'])
    with open('header.txt', 'w') as f:
        f.write(leshopch['leshopch'])
    tab.close()
    print('token expired')
    print('\n')

def initialize_browser(host: str = '127.0.0.1', port: int = 1941) -> Chromium:
    return Chromium(f'{host}:{port}')

def get_cleaned_data(txt):
    try:
        clean_text = re.sub(r"<.*?>", "", txt)
        return clean_text
    except:
        return txt

def get_images(product):
    image_urls = []
    try:
        images = _.get(product, 'images', [])
        if images != []:
            for image in images:
                im = _.get(image, 'url', None)
                if im == None:
                    continue
                image_urls.append(im)
    except Exception as e:
        pass
    return image_urls

def get_product_nutrition(product):
    dic = {}
    nutrientsTable = _.get(product, 'productInformation.nutrientsInformation', {})

    cleaned_data = copy.deepcopy(nutrientsTable)
    # Clean empty string values in both tables
    # for table_key in ["nutrientsTableV2"]:
    rows = _.get(cleaned_data, 'nutrientsTableV2.rows', {})
    for row in rows:
        row["values"] = [v for v in row["values"] if v.strip() != ""]

    try:
        del cleaned_data['isAnalyticalConstituents']
    except:
        pass
    try:
        del cleaned_data['nutrientsTable']
    except:
        pass
    return cleaned_data


def get_product_info(product):
    product_info = {}

    brand_label = []
    labels = _.get(product, 'productInformation.mainInformation.labels', [])
    if labels != []:
        for label in labels:
            slug = label.get("slug", None)
            if slug == None:
                continue
            brand_label.append(slug)
    else:
        labels = _.get(product, 'productInformation.mainInformation.brand.slug', None)
        brand_label.append(labels)

    product_info['brand_and_label'] = brand_label
    product_info['quantityPrice'] = _.get(product, 'offer.quantityPrice', None)
    product_info['characteristics'] = _.get(product, 'productInformation.mainInformation.nutritionalCharacteristicsV2[0].name', None)
    product_info['ingredients'] = get_cleaned_data(_.get(product, 'productInformation.mainInformation.ingredients', None))
    product_info['allergens'] = _.get(product, 'productInformation.mainInformation.allergens', None)
    product_info['origin'] = _.get(product, 'productInformation.mainInformation.origin', None)
    product_info.update(_.get(product, 'productInformation.otherInformation', {}))
    mcheck = _.get(product, 'productInformation.mainInformation.mcheck', {})

    if mcheck != {}:
        for key in mcheck:
            Footprint = _.get(mcheck, key, {})
            if Footprint != {}:
                try:
                    del Footprint['image']
                except:
                    pass
                try:
                    del Footprint['stackImage']
                except:
                    pass

    product_info['mcheck'] = mcheck

    return product_info

def get_cat_and_subcat(product):
    cat_subcat = _.get(product, 'breadcrumb', [])
    category = None
    sub_cat = []
    if cat_subcat != []:
        for pos, cs in enumerate(cat_subcat):
            data = _.get(cs, 'name', [])
            if pos == 0:
                category = data.strip()
            else:
                sub_cat.append(data.strip())
    return category, sub_cat

def chunk_list(data, chunk_size=500):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def get_pdp(pdp_id):
    global global_headers
    json_data = {
        'offerFilter': {
            'storeType': 'OFFLINE',
            'warehouseId': 1,
            'region': 'national',
        },
        'productFilter': {
            'migrosIds': [
                pdp_id,
            ],
        },
    }

    max_retries = 3
    product_dict = {}
    for attempt in range(max_retries):
        try:
            response = requests.post(
                'https://www.migros.ch/product-display/public/v3/product-detail',
                # cookies=cookies,
                headers=global_headers,
                json=json_data,
                impersonate='chrome120'
            )
        except:
            time.sleep(2)
            continue
        if response.status_code == 200:

            for product in response.json():
                product_dict['url'] = 'https://www.migros.ch/en/product/' + pdp_id
                # print(product_dict['url'])
                product_dict['product_name'] = _.get(product, 'title', None)
                # print(product_dict['product_name'])
                product_dict['brand'] = _.get(product, 'brand', None)
                product_dict['weight'] = _.get(product, 'offer.quantity', None)
                product_dict['review_count'] = _.get(product, 'productInformation.mainInformation.rating.nbReviews', None)
                product_dict['rating'] = _.get(product, 'productInformation.mainInformation.rating.nbStars', None)
                product_dict['category'], product_dict['sub_categories'] = get_cat_and_subcat(product)
                product_dict['product_information'] = get_product_info(product)
                product_dict['product_nutrition'] = get_product_nutrition(product)
                product_dict['selling_price'] = _.get(product, 'offer.price.effectiveValue', None)
                product_dict['discount'] = None
                product_dict['mrp'] = product_dict['selling_price']
                product_dict['product_availability_status'] = None

                product_dict['product_images'] = get_images(product)

                if product_dict['brand'] == None:
                    try:
                        product_dict['brand'] = product_dict['product_information']['brand_and_label'][0]
                    except:
                        product_dict['brand'] = None

                product_dict['promotional_offers_and_discounts'] = None
                product_dict['digital_coupons_and_voucher_codes'] = None
                product_dict['store_locations_and_inventory_data'] = None
            break

        else:
            with headers_lock2:  # only one thread refreshes headers at a time
                get_header()
            continue

    return product_dict


def pro_id(uid):
    global got_pro_id_list
    date = datetime.today().strftime("%Y-%m-%d")
    json_data = {
        'offerFilter': {
            'storeType': 'OFFLINE',
            'region': 'national',
            'ongoingOfferDate': f'{date}T00:00:00',
        },
        'productFilter': {
            'uids': uid,
        },
    }
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(
                'https://www.migros.ch/product-display/public/v4/product-cards',
                # cookies=cookies,
                headers=global_headers,
                json=json_data,
                impersonate="chrome120"
            )
        except:
            time.sleep(2)
            continue


        lis = []
        if response.status_code == 200:
            for product in response.json():
                migrosId = _.get(product, 'migrosId', None)
                if migrosId != None:
                    lis.append(migrosId)
            break
        else:
            with headers_lock1:  # only one thread refreshes headers at a time
                get_header()
            continue  # retry request after refreshing headers

    got_pro_id_list.extend(lis)

def get_pro_ids(uids):
    global global_headers
    uids = list(chunk_list(uids, 500))

    num_threads = 10
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(pro_id, [row for row in uids])


def get_cat_id(slug):
    global global_headers

    try:
        with open('header.txt', 'r') as f:
            new_headers = f.read()
    except:
        return None

    global_headers['leshopch'] = new_headers
    try:
        response = requests.get(
            f'https://www.migros.ch/product-display/public/v1/categories-breadcrumb?slug={slug}',
            # params=params,
            # cookies=cookies,
            headers=global_headers,
            impersonate='chrome120'
        )
        # print(response.text)
        if response.status_code == 200:
            return _.get(response.json(), 'id', None)
        elif response.status_code == 404:
            return 'not found'
        else:
            get_header()
            get_cat_id(slug)
    except:
        time.sleep(2)
        get_cat_id(slug)


def get_search_uuids(keyword, _from=0):
    global global_headers, ids_list

    json_data = {
        'regionId': 'national',
        'language': 'en',
        'productIds': [],
        'query': keyword,
        'sortFields': [],
        'sortOrder': 'asc',
        'from': 0,
        'limit': 1000,
        'algorithm': 'DEFAULT',
        'filters': {},
        'myProductsOverride': [],
    }
    flag = False
    while True:
        json_data['from'] = _from
        try:
            response = requests.post(
                f'https://www.migros.ch/onesearch-oc-seaapi/public/v5/search',
                # cookies=cookies,
                headers=global_headers,
                json=json_data,
                impersonate="chrome120"
            )
        except:
            time.sleep(2)
            continue

        if response.status_code == 200:
            get_json_data = response.json()

            numberOfProducts = _.get(get_json_data, 'numberOfProducts', None)
            offset = _.get(get_json_data, 'offset', None)
            if numberOfProducts == None or offset == None:
                break
            id_lis = _.get(get_json_data, 'productIds', [])
            if id_lis != []:
                ids_list.extend(id_lis)

            if numberOfProducts == offset:
                break
            else:
                _from += 1000
        else:
            flag = True
            break
    if flag:
        get_header()
        get_pro_uuids(keyword, _from)

    return ids_list


def get_pro_uuids(cat_id, _from=0):
    global global_headers, ids_list
    json_data = {
        'regionId': 'national',
        'language': 'en',
        'sortFields': [],
        'sortOrder': 'asc',
        'from': 0,
        'limit': 1000,
        'filters': {},
        'searchAlgorithm': 'DEFAULT',
    }
    flag = False
    while True:
        json_data['from'] = _from
        try:
            response = requests.post(
                f'https://www.migros.ch/product-display/public/web/v1/products/category/{cat_id}/search',
                # cookies=cookies,
                headers=global_headers,
                json=json_data,
                impersonate="chrome120"
            )
        except:
            time.sleep(2)
            continue

        if response.status_code == 200:
            get_json_data = response.json()

            numberOfProducts = _.get(get_json_data, 'numberOfProducts', None)
            offset = _.get(get_json_data, 'offset', None)
            if numberOfProducts == None or offset == None:
                break
            id_lis = _.get(get_json_data, 'productIds', [])
            if id_lis != []:
                ids_list.extend(id_lis)

            if numberOfProducts == offset:
                break
            else:
                _from += 1000
        else:
            flag = True
            break
    if flag:
        get_header()
        get_pro_uuids(cat_id, _from)

    return ids_list

def migros_main(slug):
    print("start running...")

    cat_id = get_cat_id(slug)
    if cat_id == 'not found':
        uids = get_search_uuids(slug)
        if uids != []:
            get_pro_ids(uids)
            pdp_ids = got_pro_id_list[:100]
            if pdp_ids != []:
                num_threads = 100
                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    main_list = list(executor.map(get_pdp, [row for row in pdp_ids]))
                return main_list
            else:
                return []
        else:
            return []
    elif cat_id is not None:
        uids = get_pro_uuids(cat_id)
        if uids != []:
            get_pro_ids(uids)
            pdp_ids = got_pro_id_list[:100]
            if pdp_ids != []:
                num_threads = 100
                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    main_list = list(executor.map(get_pdp, [row for row in pdp_ids]))
                got_pro_id_list.clear()
                ids_list.clear()
                return main_list
            else:
                return []
        else:
            return []
    else:
        migros_main(slug)