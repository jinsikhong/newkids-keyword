from bs4 import BeautifulSoup
import requests
import csv

# def req():
# 	try:
# 		response = requests.get("https://kids.donga.com/?ptype=article&no=20230906132905400066&psub=news&gbn=01")
# 		if response.status_code == 200:
# 			print('f[INFO] STATUS CODE IS 200')
# 			return response
# 		else:
# 			raise Exception(f'[ERROR]: {response.status}')
# 	except Exception as e:
# 		return

# def soup_parse(response):
# 	print(response)
# 	html = response
# 	soup = BeautifulSoup(html, 'html.parser')
# 	print("######################")
# 	print(soup)

# def main():
# 	try:
# 		response = req()
# 		parse = soup_parse(response)
# 	except Exception as e:
# 		print(e)


# main()

url = "https://kids.donga.com/?ptype=article&no=20230906123101611065&psub=news&gbn=01"

filename = '어린이조선일보.csv'
f = open(filename, 'w', encoding='utf-8-sig', newline='')
writer = csv.writer(f)

response = requests.get(url)

if response.status_code == 200:
	html = response.text
	soup = BeautifulSoup(html, 'html.parser')
	# 제목
	title = soup.select_one('body > div.wrap_all > div.content > div.page_area > div.at_cont > div.at_cont_title > div.at_info > ul > li.title').get_text()
	# 본문
	at_content = soup.select_one('div.at_content')
	contents = at_content.select('p > span')
	# for content in contents:
	# 	print(content)

	imgs = at_content.select('p > span > img')
	for img in imgs:
		print(img['src'])

	sub_title = soup.select_one('p.at_sub_title')
	writer = soup.select_one('li.writer').get_text().strip()
	published_date = soup.select_one('li.upload_date').get_text().strip()
	thumbnail_img = soup.select_one
	# imgs = at_content.select_one('p > img')
	# print(imgs['src'])
	# print(soup.select_one('div.at_cont_title > div.at_info'))