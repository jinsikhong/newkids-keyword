import scrapy
from datetime import datetime, timedelta 
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
        today = datetime.now().date() - timedelta(days=1)
        websites = ["조선", "중앙", "동아", "어린이"]
        
        for website in websites:
            if website == "조선":
                j = 1
                while j:
                    url = f'http://kid.chosun.com/list_kj.html?catid=1&pn={j}'
                    response = requests.get(url).text
                    soup = BeautifulSoup(response, 'html.parser')
                    # 해당 페이지(j)의 기사 작성 날짜 리스트 
                    chosun_date_list = []
                    for date in soup.select('div.con > div.date_author > span.date'):
                        chosun_date_list.append(date.get_text())
                    chosun_date = datetime.strptime(chosun_date_list[0],'%Y.%m.%d %H:%M').date()
                    # 페이지의 첫번째 기사가 오늘 날짜 이전이면 해당 페이지 크롤링 할 필요 X
                    if today == chosun_date:
                        j += 1
                        yield scrapy.Request(url=url, callback=self.chosun_crawl_page, headers=headers)
                    else: break
            # 소년중앙
            elif website == "중앙":
                k = 1
                while k:
                    url = f'https://sojoong.joins.com/archives/category/news/report/page/{k}'
                    response = requests.get(url).text
                    soup = BeautifulSoup(response, 'html.parser')
                    joong_date_list = []
                    for date in soup.select('td.title > p > span'):
                        joong_date_list.append(date.get_text())
                    joong_date = datetime.strptime(joong_date_list[0],'%Y.%m.%d').date()
                    if today == joong_date:
                        k += 1
                        yield scrapy.Request(url=url, callback=self.joong_crawl_parse, headers=headers)
                    else: break

            # # 동아일보
            elif website == "동아":
                m = 1
                while m:
                    headers= {
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
                        'referer': 'http://kids.donga.com/'
                    }          
                    url = f'https://kids.donga.com/?page_no={m}&ptype=article&psub=news&gbn=01'
                    response = requests.get(url).text
                    soup = BeautifulSoup(response, 'html.parser')
                    donga_date_list = []
                    for date in soup.select('body > div.wrap_all > div.content > div.page_area > div.article_list > ul > li > dl > dd:nth-child(3)'):
                        donga_date_list.append(date.get_text())
                    donga_date = datetime.strptime(donga_date_list[0],'%Y-%m-%d %H:%M:%S').date()
                    if today == donga_date:
                        m += 1
                        yield scrapy.Request(url=url, callback=self.donga_crawl_parse, headers=headers)
                    else: break
            # 어린이조선일보 
            elif website == "어린이":
                child_categorys = ["S2N1", "S2N2", "S2N3", "S2N5", "S2N6", "S2N7"] # category: 생활경제, 동화경제, 엄마경제, 과학소식, 시사용어, 역사
                for c in child_categorys:
                    n = 1
                    while n < 2:
                        headers= {
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
                        }      
                        url = f'https://www.econoi.com/news/articleList.html?page={n}&total=326&box_idxno=&sc_sub_section_code={c}&view_type=sm'
                        response = requests.get(url).text
                        soup = BeautifulSoup(response, 'html.parser')
                        child_date_list = []
                        for date in soup.select('#section-list > ul > li > span > em.replace-date'):
                            child_date_list.append(date.get_text())
                        child_date = datetime.strptime(child_date_list[0],'%Y-%m-%d').date()
                        if today != child_date:
                            n += 1
                            yield scrapy.Request(url=url, callback=self.child_crawl_parse, headers=headers)
                        else: break

    # 조선일보
    def chosun_crawl_page(self,response):
        """
        start_request함수로부터 response 받아서
        크롤링할 기사 url과 썸네일 파일을 추출하고
        parse함수로 request 보냄
        """
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        } 
        today = datetime.now().date() - timedelta(days=1)
        # 한페이지에 기사가 10개
        for i in range(1,11):
            # 기사 작성날짜 
            article_date_list = response.xpath(f'//*[@id="container"]/section[2]/article/ul/li[{i}]/div[2]/div[3]/span[2]/text()').extract()
            article_date = datetime.strptime(article_date_list[0],'%Y.%m.%d %M:%H').date()
            # 오늘 날짜랑 다르면 break
            if today != article_date:
                break
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
            # 제목이 있을 때만 크롤링
            if item['title']:
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
                        date_text = date_text[:-1]
                    item['published_date'] = date_text
                # 1개 이하이면 날짜만 있음
                else:
                    item['writer'] = '어린이조선일보'
                    date_text = response.xpath('//*[@id="container"]/section[1]/div[3]/div[1]/span[1]/text()')[0].extract().replace('\r\n', '')[9:]
                    date_text = re.sub(r'^\s+|\s+$', '', date_text)
                    if "수정" in date_text:
                        date_text = re.findall(r'\d{4}\.\d{2}\.\d{2} \d{2}:\d{2} ', date_text)
                        date_text = date_text[:-1]
                    item['published_date'] = date_text
                
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
                item['content'] = str(content_regex)
                
                time.sleep(random.uniform(0.2, 1.1))
                
                yield item


    def joong_crawl_parse(self,response):
        """
        start_request함수로부터 response 받아서
        크롤링할 기사 url과 썸네일 파일을 추출하고
        parse함수로 request 보냄
        """
        today = datetime.now().date() - timedelta(days=1)
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }

        # 한페이지에 기사가 10개
        for i in range(1, 11):
            # 기사 작성날짜
            article_date_list = response.xpath(f'//*[@id="list_mypost"]/table/tbody/tr[{i}]/td[1]/p/span/text()').extract()
            article_date = datetime.strptime(article_date_list[0],'%Y.%m.%d').date()
            # 오늘 날짜랑 다르면 break
            if today != article_date:
                break
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
            # 제목이 존재할때만
            if title_regex:
                item['title'] = title_regex
                item['sub_title'] = ''
                item['thumbnail_img'] = str(response.meta['thumb'])
                item['published_date'] = response.xpath('//*[@id="main"]/div[1]/div[1]/span/text()')[0].extract()
                item['html_content'] = response.xpath('//*[@id="content"]/div[1]').get()
                item['writer'] = '소년중앙'
                
                # 본문의 p태그 개수를 구하기 위해서 content_div의 x_path 변수를 따로 생성
                content_div = response.xpath('//*[@id="content"]/div[1]').get()
                content_split = re.sub(r'<[^>]*>', '', content_div)
                content_regex = re.sub(r'^\s+|\s+$', '', content_split)
                item['content'] = str(content_regex)
                
                yield item

    # 소년동아일보
    def donga_crawl_parse(self,response):
        """
        start_request함수로부터 response 받아서
        크롤링할 기사 url과 썸네일 파일을 추출하고
        parse함수로 request 보냄
        """
        today = datetime.now().date() - timedelta(days=1)
        headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }
    
        for i in range(1, 11):
            # 기사 작성날짜
            article_date_list = response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[4]/ul/li[{i}]/dl/dd[2]/text()').extract()
            article_date = datetime.strptime(article_date_list[0],'%Y-%m-%d %H:%M:%S').date()
            # 오늘 날짜랑 다르면 break
            if today != article_date:
                break
            try:
                url = response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[4]/ul/li[{i}]/dl/dt/a/@href')[0].extract()
                if response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[4]/ul/li[{i}]/a/img/@src'):
                    thumbnail_img_url = 'http://kids.donga.com' + response.xpath(f'/html/body/div[2]/div[2]/div[1]/div[4]/ul/li[{i}]/a/img/@src')[0].extract()[1:]
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
            if title:
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

                # 소제목
                sub_title = soup.select_one('p.at_sub_title').get_text()

                # 기자 이름
                writer = soup.select_one('li.writer').get_text().strip()
                if len(writer) > 50 or not writer:
                    writer = "어린이동아"
                # 작성 날짜
                published_date = soup.select_one('li.upload_date').get_text().strip()
                # html content 부분 추출
                html_at_info = soup.select('div.at_cont_title > div.at_info')
                html_at_sub_title = soup.select('div.cont_view > p')
                html_content = at_content.select('p')

                item['thumbnail_img'] = response.meta['thumb']
                item['title'] = title
                item['sub_title'] = sub_title
                item['writer'] = writer
                item['published_date'] = published_date
                item['content'] = str(content)

                # content 부분 join
                join_content = ''
                for i in range(len(html_at_sub_title)):
                    join_content += str(html_at_sub_title[i])
                for i in range(len(html_content)):
                    join_content += str(html_content[i])

                item['html_content'] = str(join_content)
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

        today = datetime.now().date() - timedelta(days=1)

        # 한페이지에 기사 20개
        for i in range(1, 21):
            # 기사 작성날짜
            article_date_list = response.xpath(f'//*[@id="section-list"]/ul/li[{i}]/span/em[1]/text()').extract()
            article_date = datetime.strptime(article_date_list[0],'%Y-%m-%d').date()
            # 오늘 날짜랑 다르면 break
            if today != article_date:
                break
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
            item['title'] = str(response.xpath('//*[@id="article-view"]/div/header/div/h1/text()').getall())[1:-1]
            if item['title']:
                item['published_date'] = response.xpath('//*[@id="articleViewCon"]/div[1]/div[2]/ul/li[1]/text()').getall()[0][3:]
                
                item['sub_title'] = str(response.xpath('//*[@id="articleViewCon"]/div[1]/div[2]/h2/text()').getall())[1:-1]
                item['writer'] = '어린이경제신문'

                # html content 부분 추출
                html_string = response.xpath('//*[@id="articleViewCon"]/div[1]/div[2]').get()
                # 정규표현식으로 제거
                split_string = re.sub(r'<!--.*?-->', '', html_string)
                split_string_1 = re.sub(r'<button[^>]*>.*?</button>', '', split_string, flags=re.DOTALL)
                split_string_2 = re.sub(r'<article class="writer"[^>]*>.*?</article>', '', split_string_1, flags=re.DOTALL)
                split_string_3 = re.sub(r'<table[^>]*>.*?</table>', '', split_string_2, flags=re.DOTALL)
                split_string_4 = split_string_3.split('<div class="clearfix">')[0]
                item['html_content'] = str(split_string_4)
                # 본문의 p태그 개수를 구하기 위해서 content_div의 x_path 변수를 따로 생성
                # 본문
                content_div = response.xpath('//*[@id="article-view-content-div"]').get()
                content_split = re.sub(r'<[^>]*>', '', content_div)
                content_regex = re.sub(r'^\s+|\s+$', '', content_split)
                content_regex = re.sub(r'[^가-힣0-9a-zA-Z]', ' ', content_regex)
                content_regex = re.sub(r"'", ' ', content_regex)
                
                item['content'] = str(content_regex)
                
                yield item

    def __init__(self, *args, **kwargs):
        """
        url 중복처리하는 함수
        크롤링 함수가 시작되고 끝날 때까지만 체크
        다시 시작하면 리셋되므로 이전 데이터는 중복처리하지 못함
        """
        super(NewsCrawlerSpider, self).__init__(*args, **kwargs)
        self.crawled_urls = set()