import scrapy
import csv
from scrapy.crawler import CrawlerProcess

class MoviesSpider(scrapy.Spider):
    name = "movies_spider"
    items = []

    def start_requests(self):
        URL = "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_годам"
        yield scrapy.Request(url=URL, callback=self.parse_pages_years)

    def parse_pages_years(self, response):
        years_movies = response.css('div[class="CategoryTreeItem"] a::attr(href)').extract()
        for url_year in years_movies:
            url_year = response.urljoin('https://ru.wikipedia.org' + url_year)
            yield response.follow(url=url_year, callback=self.parse_pages_movies)     

    def parse_pages_movies(self, response):
        movies_on_page = response.css('div[id="mw-pages"]  div[class="mw-category-group"]  li a::attr(href)').extract()
        for url_movie in movies_on_page:
            url_movie = response.urljoin('https://ru.wikipedia.org' + url_movie)
            yield response.follow(url=url_movie, callback=self.parse_movie)
        next_page = response.css('div[id="mw-pages"] > a:last-of-type::attr(href)').extract_first()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse_pages_movies)

    def parse_movie(self, response):
         item = {
                'title': response.css('span[class="mw-page-title-main"]::text').extract_first(),
                'genre': response.css('td > span[data-wikidata-property-id="P136"] a::attr(title)').extract_first(),
                'producer': response.css('td > span[data-wikidata-property-id="P57"] a::attr(title)').extract_first(),
                'country': response.css('td > span[data-wikidata-property-id="P495"] a::attr(title)').extract_first(),
                'year': response.css('span[class="dtstart"]::text').extract_first(),
            }
         self.items.append(item)

def save_as_csv(items):
    with open('movies_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['title', 'genre', 'producer', 'country', 'year']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow(item)

if __name__ == "__main__":
    process = CrawlerProcess(settings={
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(MoviesSpider)
    process.start()
    save_as_csv(MoviesSpider.items)
