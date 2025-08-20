import json
import requests
from parsel import Selector
from concurrent.futures import ThreadPoolExecutor, as_completed

def aldi_main(search_keyword):
    print("start running...")
    if 'http' in search_keyword:
        search_url = search_keyword
        last_segment = search_url.rstrip("/").split("/")[-1]
        if last_segment.isdigit():
            full_data = get_aldi_product_data(search_url)
        else:
            full_data = get_aldi_search(search_url)

    else:
        # It's a search keyword
        search_url = f'https://www.aldi-now.ch/de/search?q={search_keyword}'
        full_data = get_aldi_search(search_url)

    return full_data

def get_aldi_search(search_url):
    response_text = None
    status_code = 500
    for i in range(0,10):
        try:
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'priority': 'u=0, i',
                'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            }
            params = {
                'page': '1',
                'ipp': '72',
            }
            response = requests.get(search_url, params=params, headers=headers)
            status_code = response.status_code
            response_text = response.text
            # print("aldi plp response - ", status_code)
            if status_code == 200:
                break
        except Exception as e:
            print(e)
    if status_code == 200:
        dom = Selector(text=response_text)
        product_data_list = []
        url_list = []
        all_products = dom.xpath('//div[@class="grid grid--stretch product-list"]/product-item')
        for product in all_products:
            product_url = 'https://www.aldi-now.ch' + product.xpath('.//div/a/@href').get('')
            if product_url not in url_list:
                url_list.append(product_url)
        else:
            with ThreadPoolExecutor(max_workers=20) as executor:  # tune max_workers based on system/network
                future_to_url = {executor.submit(get_aldi_product_data, url): url for url in url_list}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                        if data:  # only append valid data
                            product_data_list.append(data)
                    except Exception as e:
                        print(f"⚠️ Error fetching {url}: {e}")
            return product_data_list
    else:
        return 'response issue..'

def get_aldi_product_data(product_url):
    response = None
    response_text = None
    status_code = 500
    for i in range(0,8):
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            # 'cookie': 'OptanonAlertBoxClosed=2025-08-19T06:20:06.232Z; _gcl_au=1.1.2135199864.1755584406; _ga=GA1.1.2024662837.1755584407; _tt_enable_cookie=1; _ttp=01K30FR99PHTE7XX2D7C9PJJGA_.tt.1; _pin_unauth=dWlkPU56ZzFOV0kwT1RjdE9UZzROaTAwWldJMkxUazBaRGN0TW1FME1tSmtNamhsTnpSaA; _fbp=fb.1.1755584407098.383489413791838639; scarab.visitor=%225E51D152DA2EAF93%22; www-aldi-now-ch=3eb88450603051c5b5995ae42123e780; tfpsi=de9f415b-41c5-4f46-8737-a08404bf0e47; scarab.profile=%228911181742081%7C1755599560%7C287874%7C1755589951%7C682525%7C1755589911%7C8821350498305%7C1755586710%7C8890603962369%7C1755586656%7C580022%7C1755586634%7C8855031382017%7C1755585101%22; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Aug+19+2025+16%3A20%3A07+GMT%2B0530+(India+Standard+Time)&version=202403.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=4d54e8c2-df85-48b5-ba55-0b5e0f4e7001&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C1%3A1%2CC2%3A1%2CC3%3A1&intType=1&geolocation=%3B&AwaitingReconsent=false; last-visit=1755600607; _uetsid=8cd71ee07cc411f0bc27c1ba3092a2ad; _uetvid=8cd742507cc411f0b1423dffc31d0d19; ttcsid=1755599547597::UGBTggzDH7LovznW2AI7.3.1755600608162; ttcsid_CRJUP2JC77UDCGHCTDDG=1755599547596::2-BVjk_J2pEoC6jjezZm.3.1755600608369; _ga_GG43EN406E=GS2.1.s1755599546$o4$g1$t1755600609$j57$l0$h627422882',
        }
        response = requests.get(
            product_url,
            headers=headers,
        )
        response_text = response.text
        status_code = response.status_code
        # print("aldi pdp response - ", status_code)
        if status_code == 200:
            break

    if status_code == 200:
        dom = Selector(text=response_text)
        item = {}
        try:
            item['url'] = product_url
        except:
            item['url'] = ''
        try:
            item['product_name'] = dom.xpath('//h1[@class="title title--product"]/text()').get('').strip()
        except:
            item['product_name'] = ''
        try:
            item['brand'] = 'Aldi'
        except:
            item['brand'] = ''
        try:
            item['weight'] = dom.xpath('//div[@class="text-secondary spacing-right"]//text()').get('').strip() + " | " + dom.xpath('//div[@class="text-secondary"]//text()').get('').strip()
        except:
            item['weight'] = ''
        try:
            item['review_count'] = None
        except:
            item['review_count'] = ''
        try:
            item['rating'] = None
        except:
            item['rating'] = ''
        try:
            cat_list = []
            allcategories = dom.xpath('//ul[@class="breadcrumb breadcrumb--no-border"]/li')
            for cats in allcategories:
                category = cats.xpath('.//text()').get('').strip()
                cat_list.append(category)
            item['category'] = cat_list[1]
        except:
            cat_list = []
            item['category'] = ''
        try:
            item['sub_categories'] = cat_list[2:-1]
        except:
            item['sub_categories'] = ''
        try:
            item['product_information'] = ''.join(dom.xpath('//div[@class="tags-and-product-description"]/div//text() | //div[@class="ingredients-and-allergens__content"]/div//text()').getall()).strip()
        except:
            item['product_information'] = ''
        try:
            item['product_nutrition'] = None
        except:
            item['product_nutrition'] = ''
        try:
            item['selling_price'] = float(dom.xpath('//span[@class="js-volume-price__price volume-price__price "]//text()').get('').strip())
        except:
            item['selling_price'] = ''
        try:
            item['discount'] = None
        except:
            item['discount'] = ''
        try:
            item['mrp'] = float(dom.xpath('//span[@class="js-volume-price__price volume-price__price "]//text()').get('').strip())  # keep as string to avoid commas
        except:
            item['mrp'] = ''
        try:
            item['product_availability_status'] = True  # True / False format
        except:
            item['product_availability_status'] = False
        try:
            item['product_images'] = dom.xpath('//a[@data-zoom-id="pdp-gallery"]/@href').getall()
        except:
            item['product_images'] = ''
        try:
            item['promotional_offers_and_discounts'] = None
        except:
            item['promotional_offers_and_discounts'] = ''
        try:
            item['digital_coupons_and_voucher_codes'] = None
        except:
            item['digital_coupons_and_voucher_codes'] = ''
        try:
            item['store_locations_and_inventory_data'] = None
        except:
            item['store_locations_and_inventory_data'] = ''
        return item
    else:
        return {}
