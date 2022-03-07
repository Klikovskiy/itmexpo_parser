from bs4 import BeautifulSoup
import requests
import re
import json
import time
import asyncio
import aiohttp
import datetime
import sys
import pandas as pd

START = time.monotonic()
start_time = time.time()
now = datetime.datetime.now()


class RateLimiter:
    RATE = 2
    MAX_TOKENS = 2

    def __init__(self, client):
        self.client = client
        self.tokens = self.MAX_TOKENS
        self.updated_at = time.monotonic()

    async def get(self, *args, **kwargs):
        await self.wait_for_token()
        now = time.monotonic() - START
        print(f'Прошло {now:.0f} сек: работаю над ссылкой {args[0]}')
        return self.client.get(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.RATE
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.MAX_TOKENS)
            self.updated_at = now


def json_links_table_base_open():
    with open('./tools/links_base.json', 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)
        return data['links_base']


def links_grab():
    links_list = []  # список со ссылками

    for one_num_links in range(1, 23):
        time.sleep(1.5)
        url = f'https://www.itmexpo.ru/about/participants/?PAGEN_1={one_num_links}'
        print(f'Нахожусь тут {url}')
        headers = {
            'accept-ranges': 'none',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          'Chrome/94.0.4606.71 Safari/537.36'
        }
        req = requests.get(url, headers=headers)
        html = req.text
        bs0bj = BeautifulSoup(html, 'lxml')

        # Находит ссылки на одной странице
        find_div_block_info = bs0bj.find_all('div', {'class': 'list-participant__text d-cb'})
        for link in find_div_block_info:
            try:
                pattern = r'(/about/participants/detail.php[?]ID=\w+)$'
                links_list.append('https://www.itmexpo.ru' + str(re.search(pattern, link.find('a')['href']).group()))
            except AttributeError:
                pass

        # Открывает исходники данных
        links_base_dict = {}

        try:
            with open('./tools/links_base.json', 'r', encoding='utf-8') as json_file:
                old_data_dict = json.load(json_file)
        except json.decoder.JSONDecodeError:
            old_data_dict = {}
            pass

        # Объединяет старые и новые данные
        links_base_dict['links_base'] = links_list  # новые
        new_date_dict = old_data_dict | links_base_dict

        with open('./tools/links_base.json', 'w', encoding='utf-8') as file:
            json.dump(new_date_dict, file, indent=4, ensure_ascii=False)


itmexpo_link_list = []
org_name_list = []
address_list = []
email_list = []
phone_list = []
web_site_list = []


async def find_date_in_html(client, idx, url_link):
    async with await client.get(url_link) as resp:
        resp_text = await resp.text()
        url = url_link
        headers = {
            'accept-ranges': 'none',
            'user-agent': 'Mozilla/5.0 (Linux; U; Android 9; ru-ru; Redmi Note 8 Build/PKQ1.190616.001) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.116 Mobile Safari/537.36 XiaoMi/MiuiBrowser/12.13.0-gn'
        }
        req = requests.get(url, headers=headers)
        bs0bj = BeautifulSoup(resp_text, 'lxml')

        find_date_div = bs0bj.find('div', {'class': 'd-table-cell d-col-9 d-lnk-tdn'})

        # Находит название организации
        find_org_name = bs0bj.find('div', {'class': 'd-row'}).find('div', {
            'class': 'd-col d-col-5 d-col-sm-4 d-col-xxs-12'}).get_text()

        if len(find_date_div) == 7:

            find_address = ' '.join(find_date_div.find_all('div')[0].get_text().split())
            find_email = find_date_div.find_all('div')[1].get_text()
            find_phone = find_date_div.find_all('div')[2].get_text()

            itmexpo_link_list.append(url)
            email_list.append(find_email.replace('E-mail: ', ''))
            address_list.append(find_address.replace('Адрес: ', ''))
            phone_list.append(find_phone.replace('Телефон: ', ''))
            web_site_list.append('—')  # Нет сайта
            org_name_list.append(str(find_org_name).strip())  # Название организации


        elif len(find_date_div) == 9:
            find_address = ' '.join(find_date_div.find_all('div')[0].get_text().split())
            find_email = find_date_div.find_all('div')[1].get_text()
            find_phone = find_date_div.find_all('div')[2].get_text()
            find_web_site = find_date_div.find_all('div')[3].get_text()

            itmexpo_link_list.append(url)
            email_list.append(find_email.replace('E-mail: ', ''))
            address_list.append(find_address.replace('Адрес: ', ''))
            phone_list.append(find_phone.replace('Телефон: ', ''))
            web_site_list.append(find_web_site.replace('Web-сайт: ', ''))
            org_name_list.append(str(find_org_name).strip())  # Название организации


# Запускает сбор данных после проверки на новые карточки
async def find_license_items_handler():
    timeout = aiohttp.ClientTimeout(total=600)
    links_json_new_items_opener = list(set(json_links_table_base_open()))

    # - idx это номер итерации для моего скрипта, можно убрать если не надо
    async with aiohttp.ClientSession(timeout=timeout) as client:
        client = RateLimiter(client)
        tasks = [asyncio.ensure_future(find_date_in_html(client, idx, link)) for idx, link in
                 enumerate(links_json_new_items_opener)]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    py_ver = int(f'{sys.version_info.major}{sys.version_info.minor}')
    if py_ver > 37 and sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    print('Запуск сбора данных по ссылкам')
    asyncio.run(find_license_items_handler())

    print('Записываю данные в таблицу')
    DateScript = pd.DataFrame(
        {'Ссылка itmexpo': itmexpo_link_list,
         'Название': org_name_list,
         'Адрес': address_list,
         'E-mail': email_list,
         'Телефон': phone_list,
         'Web-сайт': web_site_list,
         }
    )

    income_sheets = {'itmexpo.ru': DateScript}
    writer = pd.ExcelWriter('itmexpo_result.xlsx',
                            engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_urls': False}})
    for sheet_name in income_sheets.keys():
        income_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()
