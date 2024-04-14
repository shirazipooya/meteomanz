import math
import re
import logging
import time
import pandas as pd
import requests


logging.basicConfig(
    level=logging.ERROR,
    filename='app.log',
    filemode='a',
    format='%(message)s'
)


class Meteomanz():
      
    def __init__(
        self,
        scale="day",
        country_code="2060",
        station_code="00000",
        hour_start="00Z",
        hour_end="23Z",
        day_start="01",
        day_end="31",
        month="01",
        year="2024",
        page="1",
        userAgent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        referer="http://www.meteomanz.com/",
        host="www.meteomanz.com",
        acceptLanguage="en-US,en;q=0.9",
        connection="keep-alive",
        accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ):        
        self.scale = scale
        self.country_code = country_code
        self.station_code = station_code
        self.hour_start = hour_start
        self.hour_end = hour_end
        self.day_start = day_start
        self.day_end = day_end
        self.month = month
        self.year = year 
        self.page = page
        self.userAgent = userAgent
        self.referer = referer
        self.host = host
        self.acceptLanguage = acceptLanguage
        self.connection = connection
        self.accept = accept
    
    
    def url(self):        
        if self.scale == "hour":
            return f"http://www.meteomanz.com/sy1?ty=hp&l=1&cou={self.country_code}&ind={self.station_code}&d1={self.day_start}&m1={self.month}&y1={self.year}&h1={self.hour_start}&d2={self.day_end}&m2={self.month}&y2={self.year}&h2={self.hour_end}&so=001&np={self.page}"        
        elif self.scale == "day":
            return f"http://www.meteomanz.com/sy2?ty=hp&l=1&cou={self.country_code}&ind={self.station_code}&d1={self.day_start}&m1={self.month}&y1={self.year}&d2={self.day_end}&m2={self.month}&y2={self.year}&so=001&np={self.page}"
    
    
    def header(self):
        return {
            "User-Agent": self.userAgent,
            "Referer": self.referer,
            "Host": self.host,
            "Accept-Language": self.acceptLanguage,
            "Connection": self.connection,
            "Accept": self.accept
        }
    
    
    def status_code(self):
        r = requests.get(url = self.url(), headers = self.header())
        return r.status_code
    
    
    def pages(self):
        r = requests.get(url = self.url(), headers = self.header())
        while True:
            if r.status_code == 200:
                try:
                    html_content = requests.get(self.url(), headers=self.header()).content
                    txt = [value for value in html_content.split(b"\n") if (value.lower().__contains__(b'showing') and value.lower().__contains__(b'results'))][0].decode('utf-8')
                    num = [int(num) for num in re.findall(r'\d+(?:\.\d+)?', txt)]
                    return math.ceil(num[-1] / num[-2])
                except:
                    return 1
            else:
                time.sleep(3)
                if self.scale == "hour":
                     logging.error(f"Error Pages: {self.year}-{self.month}-{self.day_start}, Page {self.page}")
                elif self.scale == "day":
                     logging.error(f"Error Pages: {self.year}-{self.month}, Page {self.page}")
        

    def download(self):
        r = requests.get(url = self.url(), headers = self.header())
        while True:
            if r.status_code == 200:
                df = pd.read_html(self.url(), storage_options=self.header())[0]
                if self.scale == "hour":
                    print(f"Downloaded {self.year}-{self.month}-{self.day_start}, Page {self.page}")
                elif self.scale == "day":
                    print(f"Downloaded {self.year}-{self.month}, Page {self.page}")
                return df
            else:
                time.sleep(3)
                if self.scale == "hour":
                     logging.error(f"Error Download: {self.year}-{self.month}-{self.day_start}, Page {self.page}")
                elif self.scale == "day":
                     logging.error(f"Error Download: {self.year}-{self.month}, Page {self.page}")