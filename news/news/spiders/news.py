import scrapy
from datetime import datetime 
import json
import requests
from news.items import NewsItem
from bs4 import BeautifulSoup
import time
import re
import random

class NewsCrawlerSpider(scrapy.Spider):
    
    name = "news"

    def start_requests(self):
        """
        크롤링 시작 함수,
        크롤링할 메인 url을 받아서 요청을 보냄
        """
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }

        websites = ["조선", "중앙", "동아", "어린이"]
        
        for website in websites:
            # 어린이조선일보 현재 12100페이지 (9/6기준)
            if website == "조선":
                for i in range(8602, 8603):
                # for i in range(1, 2):
                    url = f'http://kid.chosun.com/list_kj.html?catid=1&pn={i}'
                    yield scrapy.Request(url=url, callback=self.chosun_crawl_page, headers=headers)
            # 소년중앙
            # 리포트 123페이지
            # elif website == "중앙":
            #     for i in range(1, 124):
            #         url = f'https://sojoong.joins.com/archives/category/news/report/page/{i}'
            #         yield scrapy.Request(url=url, callback=self.joong_crawl_parse, headers=headers)

            # # 동아일보 2202페이지까지 있음(9/19기준)
            # elif website == "동아":
            #     donga_categorys = ["02", "03"] # category: 오늘의 시사, 사이언스, 아트&히스토리 , "01", "02", "03"
            #     # 오늘의 시사 2202페이지, 사이언스: 99페이지, 아트 226페이지
            #     for c in donga_categorys:
            #         if c == "01":
            #             j = 2230
            #         elif c == "02":
            #             j = 100
            #         elif c == "03":
            #             j == 230

            #         for i in range(1, j):
            #             headers= {
            #                 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            #                 'referer': 'http://kids.donga.com/'
            #             }          
            #             url = f'https://kids.donga.com/?page_no={i}&ptype=article&psub=news&gbn={c}'
            #             yield scrapy.Request(url=url, callback=self.donga_crawl_parse, headers=headers)

            # elif website == "어린이":
            #     # 어린이조선일보 생활경제 251페이지, 동화경제 17페이지, 엄마경제 36페이지, 과학소식 37페이지, 시사용어 20페이지, 역사 10페이지까지 있음(9/4기준)
            #     child_categorys = ["S2N1", "S2N2", "S2N3", "S2N5", "S2N6", "S2N7"] # category: 생활경제, 동화경제, 엄마경제, 과학소식, 시사용어, 역사
            #     for c in child_categorys:
            #         if c == "S2N1":
            #             j = 260
            #         else:
            #             j = 40
            #         for i in range(1, 30):
            #             headers= {
            #                 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
            #             }      
            #             url = f'https://www.econoi.com/news/articleList.html?page={i}&total=326&box_idxno=&sc_sub_section_code={c}&view_type=sm'
            #             yield scrapy.Request(url=url, callback=self.child_crawl_parse, headers=headers)

    def chosun_crawl_page(self,response):
        """
        start_request함수로부터 response 받아서
        크롤링할 기사 url과 썸네일 파일을 추출하고
        parse함수로 request 보냄
        """
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }

        # 한페이지에 기사가 10개
        for i in range(1, 11):
            try:
                url = 'http://kid.chosun.com' + response.xpath(f'//*[@id="container"]/section[2]/article/ul/li[{i}]/div[2]/div[1]/a/@href')[0].extract()
                if response.xpath(f'//*[@id="container"]/section[2]/article/ul/li[{i}]/div[1]/a/img/@src'):
                    thumbnail_img_url = response.xpath(f'//*[@id="container"]/section[2]/article/ul/li[{i}]/div[1]/a/img/@src')[0].extract()
                else:
                    thumbnail_img_url = ''
                if url:
                    yield scrapy.Request(url=url, meta={'thumb': thumbnail_img_url}, callback=self.chosun_parse, headers=headers)
            except Exception as e:
                self.logger.error("로그에러 URL: %s. Error: %s", response, str(e))

    def chosun_parse(self, response):
        """
        crawl_page함수로부터 response를 받아
        데이터를 파싱하는 함수
        """
        item_url = response.url
        if item_url not in self.crawled_urls:
            item = NewsItem()
            item['title'] = response.xpath('//*[@id="container"]/section[1]/div[2]/text()')[0].extract()
            item['thumbnail_img'] = response.meta['thumb'] 
            item['html_content'] = response.xpath('//*[@id="article"]').get()
            
            # 날짜, 기자 이름
            len_span_tags = len(response.xpath('//*[@id="container"]/section[1]/div[3]/div[1]/span').extract())

            # span 태그가 2개 이상이면 날짜, 기자 모두 있음
            if len_span_tags >= 2:
                writer_text = response.xpath('//*[@id="container"]/section[1]/div[3]/div[1]/span[1]/text()')[0].extract()
                writer_text = re.sub(r'^\s+|\s+$', '', writer_text)
                item['writer'] = writer_text
                
                date_text = response.xpath('//*[@id="container"]/section[1]/div[3]/div[1]/span[@class="date"]/text()')[0].extract().replace('\r\n', '')[9:]
                date_text = re.sub(r'^\s+|\s+$', '', date_text)
                if "수정" in date_text:
                    date_text = re.findall(r'\d{4}\.\d{2}\.\d{2} \d{2}:\d{2} ', date_text)
                item['published_date'] = date_text[:-1]
            # 1개 이하이면 날짜만 있음
            else:
                item['writer'] = ''
                date_text = response.xpath('//*[@id="container"]/section[1]/div[3]/div[1]/span[1]/text()')[0].extract().replace('\r\n', '')[9:]
                date_text = re.sub(r'^\s+|\s+$', '', date_text)
                if "수정" in date_text:
                    date_text = re.findall(r'\d{4}\.\d{2}\.\d{2} \d{2}:\d{2} ', date_text)
                item['published_date'] = date_text[:-1]
            
            # 기자이름이 50자 이상이면 어린이조선일보로
            if len(item['writer']) > 50:
                item['writer'] = "어린이조선일보"

            # 소제목
            len_subtitle = len(response.xpath('//*[@id="article"]/h3/text()').extract())
            if len_subtitle == 0:
                item['sub_title'] = ''
            else:
                subtitle = ''
                for i in range(len_subtitle):
                    subtitle += response.xpath(f'//*[@id="article"]/h3/text()')[i].extract()
                item['sub_title'] = subtitle

            # 본문의 p태그 개수를 구하기 위해서 content_div의 x_path 변수를 따로 생성
            content_div = response.xpath('//*[@id="article"]').get()
            content_split = re.sub(r'<[^>]*>', '', content_div)
            content_regex = re.sub(r'^\s+|\s+$', '', content_split)
            item['content'] = content_regex

            # 이미지
            img_list = []
            image_regexs = re.findall(r'<img[^>]*src="([^"]+)"[^>]*>', content_div)
            for image_regex in image_regexs:
                img_list.append(image_regex)

            img_list = str(img_list)
            item['imgs'] = img_list
            
            # time.sleep(random.uniform(0.3, 1.1))
            
            yield item

    def joong_crawl_parse(self,response):
        """
        start_request함수로부터 response 받아서
        크롤링할 기사 url과 썸네일 파일을 추출하고
        parse함수로 request 보냄
        """
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }

        # 한페이지에 기사가 10개
        for i in range(1, 11):
            try:
                url = response.xpath(f'//*[@id="list_mypost"]/table/tbody/tr[{i}]/td[1]/h2/a/@href')[0].extract()
                if response.xpath(f'//*[@id="list_mypost"]/table/tbody/tr[{i}]/td[1]/a/span/img/@src'):
                    thumbnail_img_url = response.xpath(f'//*[@id="list_mypost"]/table/tbody/tr[{i}]/td[1]/a/span/img/@src')[0].extract()
                else:
                    thumbnail_img_url = ''
                if url:
                    yield scrapy.Request(url=url, meta={'thumb': thumbnail_img_url}, callback=self.joong_parse, headers=headers)
            except Exception as e:
                self.logger.error("로그에러 URL: %s. Error: %s", response, str(e))

    def joong_parse(self, response):
        """
        crawl_page함수로부터 response를 받아
        데이터를 파싱하는 함수
        """
        item_url = response.url
        if item_url not in self.crawled_urls:
            item = NewsItem()
            title = response.xpath('//*[@id="main"]/div[1]/h1/text()')[0].extract()
            title_regex = re.sub(r'^\s+|\s+$', '', title)
            item['title'] = title_regex
            item['sub_title'] = ''
            item['thumbnail_img'] = str(response.meta['thumb'])
            item['published_date'] = response.xpath('//*[@id="main"]/div[1]/div[1]/span/text()')[0].extract()
            item['html_content'] = response.xpath('//*[@id="content"]/div[1]').get()
            item['writer'] = ''
            
            # 본문의 p태그 개수를 구하기 위해서 content_div의 x_path 변수를 따로 생성
            content_div = response.xpath('//*[@id="content"]/div[1]').get()
            content_split = re.sub(r'<[^>]*>', '', content_div)
            content_regex = re.sub(r'^\s+|\s+$', '', content_split)
            item['content'] = content_regex

            # 이미지
            img_list = []
            image_regexs = re.findall(r'<img .*?src="(.*?)".*?>', content_div)
            for image_regex in image_regexs:
                img_list.append(image_regex)
            item['imgs'] = str(img_list)
            
            yield item

    # 소년동아일보
    def donga_crawl_parse(self,response):
        """
        start_request함수로부터 response 받아서
        크롤링할 기사 url과 썸네일 파일을 추출하고
        parse함수로 request 보냄
        """
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }

        for i in range(1, 11):
            try:
                # url = response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[4]/ul/li[{i}]/dl/dt/a/@href')[0].extract()
                url = response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[3]/ul/li[{i}]/dl/dt/a/@href')[0].extract()
                # if response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[4]/ul/li[{i}]/a/img/@src'):
                if response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[3]/ul/li[{i}]/a/img/@src'):
                    # thumbnail_img_url = 'http://kids.donga.com' + response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[4]/ul/li[{i}]/a/img/@src')[0].extract()[1:]
                    thumbnail_img_url = 'http://kids.donga.com' + response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[3]/ul/li[{i}]/a/img/@src')[0].extract()[1:]
                else:
                    thumbnail_img_url = ''
                if url:
                    yield scrapy.Request(url=url, meta={'thumb': thumbnail_img_url}, callback=self.donga_parse, headers=headers)
            except Exception as e:
                self.logger.error("로그에러 URL: %s. Error: %s", response, str(e))

    # donga는 scrapy로 추출이 안 됨
    # bs4로 크롤링
    def donga_parse(self, response):
        """
        crawl_page함수로부터 response를 받아
        데이터를 파싱하는 함수
        """
        item_url = response.url
        if item_url not in self.crawled_urls:
            item = NewsItem()
            url = response.url
            bs4_response = requests.get(url)
            html = bs4_response.text
            # html 파싱
            soup = BeautifulSoup(html, 'html.parser')
            # 제목
            title = soup.select_one('body > div.wrap_all > div.content > div.page_area > div.at_cont > div.at_cont_title > div.at_info > ul > li.title').get_text()
            # 본문
            at_content = soup.select_one('div.at_content')
            contents = at_content.select('p')
            content = ''
            for con in contents:
                # 어린이동아 이후로는 불필요
                if '▶어린이동아' in con.get_text():
                    break
                content += con.get_text()

            # content 불필요한 부분 정규표현식으로 제거
            content = re.sub(r'<.*?>', '', content)
            content = re.sub("위 기사의 법적인 책임과 권한은 어린이동아에 있습니다.", "", content)
            content = re.sub(r'^\s+|\s+$', '', content)

            # img src만 추출
            imgs = at_content.select('p > img')
            imgs_1 = at_content.select('p > span > img')
            img_list = []
            for img in imgs:
                img_list.append(img['src'])
            
            for img in imgs_1:
                if img['src'] not in img_list:
                    img_list.append(img['src'])

            # 소제목
            sub_title = soup.select_one('p.at_sub_title').get_text()

            # 기자 이름
            writer = soup.select_one('li.writer').get_text().strip()
            if writer > 50:
                writer = "동아일보"
            # 작성 날짜
            published_date = soup.select_one('li.upload_date').get_text().strip()
            # html content 부분 추출
            html_at_info = soup.select('div.at_cont_title > div.at_info')
            html_at_sub_title = soup.select('div.cont_view > p')
            html_content = at_content.select('p')
            # # date에 PM, AM을 yyyy-mm-dd hh:mm 형태로 바꾸기
            # date_obj = datetime.strptime(published_date, "%Y-%m-%d %I:%M:%S %p")
            # formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")

            item['imgs'] = str(img_list)
            item['thumbnail_img'] = response.meta['thumb']
            item['title'] = title
            item['sub_title'] = sub_title
            item['writer'] = writer
            item['published_date'] = published_date
            item['content'] = content

            # content 부분 join
            join_content = ''
            for i in range(len(html_at_sub_title)):
                join_content += str(html_at_sub_title[i])
            for i in range(len(html_content)):
                join_content += str(html_content[i])

            item['html_content'] = join_content
            yield item

    # 어린이경제신문
    def child_crawl_parse(self,response):
        """
        start_request함수로부터 response 받아서
        크롤링할 기사 url과 썸네일 파일을 추출하고
        parse함수로 request 보냄
        """
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }

        # 한페이지에 기사 20개
        for i in range(1, 21):
            try:
                url = 'https://www.econoi.com' + response.xpath(f'//*[@id="section-list"]/ul/li[{i}]/h2/a/@href')[0].extract()
                # 썸네일 이미지
                if response.xpath(f'//*[@id="section-list"]/ul/li[{i}]/a/img/@src'):
                    thumbnail_img_url = response.xpath(f'//*[@id="section-list"]/ul/li[{i}]/a/img/@src')[0].extract()
                else:
                    thumbnail_img_url = ''
                if url:
                    yield scrapy.Request(url=url, meta={'thumb': thumbnail_img_url}, callback=self.child_parse, headers=headers)
            except Exception as e:
                self.logger.error("로그에러 URL: %s. Error: %s", response, str(e))

    def child_parse(self, response):
        """
        crawl_page함수로부터 response를 받아
        데이터를 파싱하는 함수
        """
        item_url = response.url
        if item_url not in self.crawled_urls:
            item = NewsItem()
            item['thumbnail_img'] = response.meta['thumb']
            item['title'] = response.xpath('//*[@id="article-view"]/div/header/div/h1/text()').getall()
            item['published_date'] = response.xpath('//*[@id="articleViewCon"]/div[1]/div[2]/ul/li[1]/text()').getall()[0][3:]
            
            item['sub_title'] = response.xpath('//*[@id="articleViewCon"]/div[1]/div[2]/h2/text()').getall()
            item['writer'] = ''

            # html content 부분 추출
            html_string = response.xpath('//*[@id="articleViewCon"]/div[1]/div[2]').get()
            # 정규표현식으로 제거
            split_string = re.sub(r'<!--.*?-->', '', html_string)
            split_string_1 = re.sub(r'<button[^>]*>.*?</button>', '', split_string, flags=re.DOTALL)
            split_string_2 = re.sub(r'<article class="writer"[^>]*>.*?</article>', '', split_string_1, flags=re.DOTALL)
            split_string_3 = re.sub(r'<table[^>]*>.*?</table>', '', split_string_2, flags=re.DOTALL)
            split_string_4 = split_string_3.split('<div class="clearfix">')[0]
            item['html_content'] = split_string_4 
            # 본문의 p태그 개수를 구하기 위해서 content_div의 x_path 변수를 따로 생성
            # 본문
            content_div = response.xpath('//*[@id="article-view-content-div"]').get()
            content_split = re.sub(r'<[^>]*>', '', content_div)
            content_regex = re.sub(r'^\s+|\s+$', '', content_split)
            
            img_list = []
            # img url 정규표현식으로 추출
            image_regexs = re.findall(r'<img[^>]*src="([^"]+)"[^>]*>', content_div)
            for image_regex in image_regexs:
                img_list.append(image_regex)
            item['imgs'] = str(img_list)
            item['content'] = content_regex

            yield item

    def __init__(self, *args, **kwargs):
        """
        url 중복처리하는 함수
        크롤링 함수가 시작되고 끝날 때까지만 체크
        다시 시작하면 리셋되므로 이전 데이터는 중복처리하지 못함
        """
        super(NewsCrawlerSpider, self).__init__(*args, **kwargs)
        self.crawled_urls = set()