import re
import json
import sys
import time

import requests
import urllib.parse
from parsel import Selector
from concurrent.futures import ThreadPoolExecutor


def clean_whitespace(text):

    if not isinstance(text, str):
        return text  # Return as-is if not a string (e.g., None, numbers)

    # Replace any form of newline/carriage return with space
    text = re.sub(r'[\r\n\t]+', ' ', text)

    # Replace multiple spaces (especially long whitespace sequences) with single space
    text = re.sub(r'\s+', ' ', text)

    # Strip leading and trailing spaces
    return text.strip()

def extract_datadome(cookie_string: str) -> str | None:
    """
    Extracts the datadome value from a Set-Cookie string.
    """
    for part in cookie_string.split(";"):
        if part.strip().startswith("datadome="):
            return part.strip().split("=", 1)[1]
    return None

def get_cookie():
    url = "https://api-js.datadome.co/js/"

    headers = {
        'sec-ch-ua-platform': '"Windows"',
        'Referer': '',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'Content-type': 'application/x-www-form-urlencoded',
        'sec-ch-ua-mobile': '?0',
    }

    data = {
        'jspl': '... your long token here ...',
        'eventCounters': '{"mousemove":28,"click":0,"scroll":0,"touchstart":0,"touchend":0,"touchmove":0,"keydown":0,"keyup":0}',
        'jsType': 'le',
        'cid': '3ThdgrYeUMU8BKQyrTnreZr26gS6NhMhEAe9LPr9rQihARTNMJesApFYGlL4l7eMvz8b0BuNZEdo6XA~HcCjuljXtHmiRWNDjyDwjB22Bu1dga0y1Kj_IvE3idELpsQD',
        'ddk': '4E2D21855D176C1885A97CE16C8EC3',
        'Referer': 'https%3A%2F%2Fwww.coop.ch%2Fen%2Ffood%2Fdrinks%2Fmineral-water%2Fmultipacks-more-than-1-liter%2Fevian-non-carbonated-mineral-water-6x15l%2Fp%2F3050447%3Fcontext%3Dsearch%23tab%3Dproduct-information',
        'request': '%2Fen%2Ffood%2Fdrinks%2Fmineral-water%2Fmultipacks-more-than-1-liter%2Fevian-non-carbonated-mineral-water-6x15l%2Fp%2F3050447%3Fcontext%3Dsearch%23tab%3Dproduct-information',
        'responsePage': 'origin',
        'ddv': '5.1.7',
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        json_data = response.json()
        return json_data.get("cookie")  # safely get cookie from response
    except Exception as e:
        print(f"Error: {e}")
        return None

def img_link_extraction(img_url_make):
    headers = {
        'sec-ch-ua-full-version-list': '"Not;A=Brand";v="99.0.0.0", "Google Chrome";v="139.0.7258.67", "Chromium";v="139.0.7258.67"',
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://www.coop.ch/en/wine/all-wines/white-wines/niederoesterreich-gruener-veltliner-reserve-sauberg-ebner-ebenauer/p/1015528003?trackingtoken=plprelevanz%7Carea2%7CA%7CPLPRelevanz%7CPLPRelevanz_U2P_Promo%7CU2PS',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'sec-ch-ua-model': '""',
        'sec-ch-device-memory': '8',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-arch': '"x86"',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': '*/*',
    }
    token = ''
    scrape_do_url = f'http://api.scrape.do?token={token}&url={urllib.parse.quote(img_url_make)}'

    img_url_req = requests.get(scrape_do_url,headers=headers)
    data = img_url_req.json()

    img_list = []
    try:
        images = data["contentJsons"]["anchors"][0]["json"]["elements"][0]["images"]
        for p in images:
            if "image" in p and "loader" in p["image"]:
                img_list.append(f"https:{p['image']['loader']}")
    except (KeyError, IndexError, TypeError):
        print("Image data not found in response")

    return img_list

def coop_main(category_keyword):

    print("start running...")
    if "http" in category_keyword and "https://www.coop.ch/en/search/?" in category_keyword:
        category_url = category_keyword
        content = pl_link_extraction(category_url)

    elif "http" not in category_keyword:
        category_url = f"https://www.coop.ch/en/search/?text={category_keyword}"
        content = pl_link_extraction(category_url)

    else:
        return "Something missing.."

    print(content)
    return content

def pl_link_extraction(category_url):
    final_result = []
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        # 'Referer': 'https://www.coop.ch/en/?srsltid=AfmBOoq6c8W5OCWfPTMXNwxXjwrHylpkSq4-SKGlDcisCoeJkNp5IH2H',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-device-memory': '8',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-full-version-list': '"Not;A=Brand";v="99.0.0.0", "Google Chrome";v="139.0.7258.67", "Chromium";v="139.0.7258.67"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        # 'Cookie': 'RDESID=711f4e46d85081af5ba8aa026ae6f84ba10802316d4729c500eca4b28240bd9e; language=en; kameleoonVisitorCode=oztwhgp4rqsngr2j; accessmode=external; _fbp=fb.1.1755584144511.387206468135379502; _gcl_au=1.1.1245419633.1755584145; tws_camp=%7B%22medium%22%3A%22(none)%22%2C%22source%22%3A%22(direct)%22%7D; _pin_unauth=dWlkPU5UUTJZVEV4TTJVdFpqSm1ZUzAwWmpVMUxUbGxNVFV0TVRRME56azVaVGt4TldFNA; baqend-speedkit-user-id=cte3cbWO2oCZNhVS8CpJftD2j; QuantumMetricUserID=19dff352915a3fc7cc718fcbfc17efbc; _gid=GA1.2.657050881.1755589752; _ga=GA1.1.473916870.1755584144; _ga_TLRGQESWDT=GS2.1.s1755591593$o1$g1$t1755591628$j25$l0$h0; JSESSIONID=4719699A96E83BC63A2509EBE0EC786C.accstorefront-64dbbb5676-6hntq; suppressRedirectToSCID=true; ROUTE=.accstorefront-64dbbb5676-6hntq; insiemeLBcookie=!3ORj2v9LQ2EJjG9zeh7k8sHEX43cZ8hPr8iNe6xkjXw/19LDSYR+PEhr1coFc/AmWUoppqWVxf3ln/CjLUtM6qqVq7RAe+AOpz8nGuhdA2Yd; ANONYMOUS_USER_SESSIONID=fc16c7c0-ec98-40ec-b32a-65e740519386; www-insiemeLBcookie=!Qwja5VS+wqI5COhCzL2R2N9u5EmB74C+4iRCdv+DrgbsBOi3vLPlnQA56oi2X3oJlBzVq9kYSlG3k8XskLT1oOEkectbuBp9w2AKS3ZH/4HS; TS01d253ce=0188e1aa7fa39fdf32d39adf136338743757f609db2284da0803a1364667eecf7b0e30cb2f4b7af14a2d6da4c4f4ecc5b92b1d3bac6f0b9dd50b74e88c4b8cda038e1e31e40ae2a1d104889353cfeb5ea40102fe1140771e193572fa5430b15e59c69cbdbc4a5cc41259af98fb6bf750cfb8a887bb127bc8e2ddda7f9850d46b52e26dd1e5f038f664480485d73adcd4be20f0908ab7fd155ecc573a3d5429b18b711dd640; _uetsid=f07ffd307cc311f08f478dc038fa1341; _uetvid=f08023907cc311f0b2fa57ef5194071b; utag_main=_sn:5$_se:6%3Bexp-session$_ss:0%3Bexp-session$_st:1755667630105%3Bexp-session$ses_id:1755665829178%3Bexp-session$_pn:1%3Bexp-session$ga4ClientId:c473916870.1755584144%3Bexp-session; _ga_FDQTHHLP9T=GS2.1.s1755665829$o5$g1$t1755665830$j59$l0$h0; tws_session=%7B%22pageCount%22%3A1%2C%22calcToggle%22%3Atrue%2C%22cv%22%3A%22%22%2C%22pageHistory%22%3A%5B%7B%22breadcrumb1%22%3A%22startseite%2Fnone%22%2C%22breadcrumb2%22%3A%22%2Fen%2F%22%2C%22time%22%3A1755665830%7D%5D%2C%22visitorType%22%3A%22Returning%22%2C%22productDetailClick%22%3A0%7D; __gads=ID=01358d680e485445:T=1755584147:RT=1755665830:S=ALNI_MbGXiAWbgcOHVaGB7-XztAazFLgUw; __gpi=UID=00001107c3db8a25:T=1755584147:RT=1755665830:S=ALNI_Mb7gSt1YU9vShSPEMj7mXrga3pfTA; __eoi=ID=c198830236d29d11:T=1755584147:RT=1755665830:S=AA-AfjbJ9KFoG5JaYZbLJjKwmG-n; QuantumMetricSessionID=7bee2ca6c35116e81c92d469a155e89a; carousel-active=carousel1; datadome=r3Qm3LYQOYY8po~1Gkl2gqAmAepg2voGWelCCtfsqQLP7UjT3ZqCRDxGhmd6lZUGOftvT_mNdd0tkXGQ0KfmF5OEXyTFdY~uT0LPVLGlPW8M5hyWmULicB_IWZY1~zjN',
    }

    update_link = "https://www.coop.ch/en/dynamic-pageload/searchresultJson"

    params = {
        'componentName': 'searchresultJson',
        'url': category_url,
        'displayUrl': category_url,
    }

    response = requests.get(update_link,
                            params=params,
                            headers=headers
                            )

    try:
        loaded_json = response.json()
    except json.JSONDecodeError:
        print("Failed to parse product list JSON")
        return []

    checking_content = loaded_json.get("html")
    if "No matches for" in checking_content:
        return "No Matches Available"

    try:
        product_list = [
            f"https://www.coop.ch{p['href']}"
            for p in loaded_json.get("contentJsons",{}).get("anchors", [])[1].get("json", {}).get("elements", [])
            if isinstance(p, dict) and "href" in p
        ]
    except:
        product_list = [
            f"https://www.coop.ch{p['href']}"
            for p in loaded_json.get("contentJsons", {}).get("anchors", [])[0].get("json", {}).get("elements", [])
            if isinstance(p, dict) and "href" in p
        ]

    with ThreadPoolExecutor(max_workers=len(product_list)) as executor:
        content = list(executor.map(pdp_data_extraction, product_list))
        try:
            content.remove(None)
        except:...
    final_result.extend(content)
    return final_result

def pdp_data_extraction(input_link):
    try:
        token = ""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
        }

        for i in range(3):
            update_link = input_link
            scrape_do_url = f'http://api.scrape.do?token={token}&url={urllib.parse.quote(update_link)}'
            response = requests.get(scrape_do_url, headers=headers, timeout=1000)
            my_response = ''
            if response.status_code != 200:
                update_link = f'https://www.coop.ch/en/dynamic-pageload/productBasicInfo?componentName=productBasicInfo&url={input_link.replace("https://www.coop.ch", "")}&displayUrl={input_link.replace("https://www.coop.ch", "")}'
                scrape_do_url = f'http://api.scrape.do?token={token}&url={urllib.parse.quote(update_link)}'
                response = requests.get(scrape_do_url, headers=headers, timeout=1000)
                json_getter = response.json()
                if json_getter.get("html"):
                    my_response = Selector(text=json_getter.get("html"))
                    break
                else:
                    print("retry attemps:", i + 1)
                    continue
            else:
                my_response = Selector(text=response.text)
                break

            # # JSON response check
            # json_getter = response.json()
            # if json_getter.get("html"):
            #     my_response = Selector(text=json_getter.get("html"))
            #     break
            # else:
            #     print("retry attemps:",i+1)
            #     continue

        if my_response:
            item = {"url": input_link}

            product_name = my_response.xpath('//h1[@class="title title--productBasicInfo"]//text()').get()
            item["product_name"] = product_name.strip() if product_name else None

            brand = my_response.xpath('//span[@class="productBasicInfo__productMeta-value-item"]/span/text()').get()
            item["brand"] = brand.strip() if brand else None

            weight1 = my_response.xpath('//span[@data-testauto="productweight"]/text()').get()
            weight2 = my_response.xpath('//span[@data-testauto="productweight"]/following-sibling::text()').get()
            item["weight"] = f"{weight1.strip()}{weight2.strip()}" if weight1 and weight2 else None

            rating = my_response.xpath('//div[@class="rating"]/span/text()').get()
            item["rating"] = float(rating.replace("Average rating: ", "").replace(" of 5", "")) if rating else None

            review = my_response.xpath('//span[@class="rating__amount"]//span[@itemprop="reviewCount"]/text()').get()
            item["review_count"] = int(review) if review else "0"

            category = my_response.xpath('//ul[@class="breadCrumb__items"]/li[2]/a/span/text()').get()
            item["category"] = category.strip() if category else None

            sub_category = my_response.xpath('//ul[@class="breadCrumb__items"]/li/a/span/text()').getall()
            item["sub_categories"] = sub_category[2:] if sub_category else None

            # Product information
            product_information_list = []
            if my_response.xpath('//div[contains(@class,"productInformation__row row")]'):
                for product_information_ls in my_response.xpath('//div[contains(@class,"productInformation__row row")]'):
                    key = product_information_ls.xpath('./h3[@class="productInformation__title col-12 col-md-3"]/text()').get()
                    value = product_information_ls.xpath('.//div[contains(@class,"productInformation__content")]//text()').getall()
                    if key and "Nutrition informatio" not in key:
                        product_information_list.append({
                            "key": key.strip(),
                            "value": clean_whitespace(" ".join(value))
                        })
            item["product_information"] = product_information_list if product_information_list else None

            # Nutrition info
            product_nutrition_list = []
            if my_response.xpath('//li[@data-testauto="nutrition-row"]'):
                for product_nutrition_ls in my_response.xpath('//li[@data-testauto="nutrition-row"]'):
                    key = product_nutrition_ls.xpath('.//span[@class="list--dotted-item__label-text"]/text()').get()
                    value = product_nutrition_ls.xpath('.//span[@class="list--dotted-item__amount"]//text()').getall()
                    if key:
                        product_nutrition_list.append({
                            "key": key.strip(),
                            "value": clean_whitespace("".join(value)).strip()
                        })
            item["product_nutrition"] = product_nutrition_list if product_nutrition_list else None

            # Price info
            selling_price = my_response.xpath('//p[@class="productBasicInfo__price-value-lead-price"]/text()').get()
            item["selling_price"] = float(selling_price.strip()) if selling_price else None

            discount = my_response.xpath('//span[@class="productBasicInfo__price-text-saving-inner"]/text()').get()
            item["discount"] = discount.strip() if discount else None

            mrp = my_response.xpath('//p[@class="productBasicInfo__price-value-lead-price-old"]/text()').get()
            item["mrp"] = float(mrp.strip()) if mrp else item["selling_price"]

            availability_text = my_response.xpath('//span[@class="productAvailability__notice"]/text()').get()
            if availability_text and ("not" in availability_text.lower() or "unavailabl" in availability_text.lower()):
                item["product_availability_status"] = False
            else:
                item["product_availability_status"] = True

            # TODO :: img request
            if "?" in input_link:
                split_url = input_link.split("?")[0]
            else:
                split_url = input_link

            img_url_make = f"{split_url}/image"
            img_list = img_link_extraction(img_url_make)
            item['product_images'] = img_list if img_list else None

            item["promotional_offers_and_discounts"] = None
            item["digital_coupons_and_voucher_codes"] = None
            item["store_locations_and_inventory_data"] = None
            return item
    except Exception as e:
        print(sys.exc_info()[2].tb_lineno)



# coop_main("wine")
