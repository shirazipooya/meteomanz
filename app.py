import time
import logging
import pandas as pd
import requests


logging.basicConfig(
    level=logging.ERROR,
    filename='app.log',
    filemode='a+',
    format='%(message)s'
)


class meteomanz():
      
    def __init__(
        self,
        scale="day",
        country_code="2060",
        station_code="00000",
        start_hour="00Z",
        end_hour="23Z",
        start_day="01",
        day_end="01",
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
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.start_day = start_day
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
            return f"http://www.meteomanz.com/sy1?cou={self.country_code}&ind={self.station_code}&d1={self.start_day}&m1={self.month}&y1={self.year}&h1={self.start_hour}&d2={self.day_end}&m2={self.month}&y2={self.year}&h2={self.end_hour}&so=001&np={self.page}"        
        elif self.scale == "day":
            return f"http://www.meteomanz.com/sy2?cou={self.country_code}&ind={self.station_code}&d1={self.day_start}&m1={self.month}&y1={self.year}&d2={self.day_end}&m2={self.month}&y2={self.year}&so=001&np={self.page}"
    
    def header(self):
        return {
            "User-Agent": self.userAgent,
            "Referer": self.referer,
            "Host": self.host,
            "Accept-Language": self.acceptLanguage,
            "Connection": self.connection,
            "Accept": self.accept
        }
    
    def download(self):
        return pd.read_html(self.url(), storage_options=self.header())[0]

        


# YEARS = [*map(str, range(2000, 2025, 1))]
# MONTH = [*map(lambda x: str(x).zfill(2), range(1, 13, 1))]
# DAY = [*map(lambda x: str(x).zfill(2), range(1, 32, 1))]
# PAGE = [*map(str, range(1, 13, 1))]

# HEADERS = create_header()

# for y in YEARS:
#     data = pd.DataFrame()
#     for m in MONTH:
#         for p in PAGE:
#             URL = create_url(
#                 country_code="2060",
#                 station_code="00000",
#                 day_start="01",
#                 day_end="31",
#                 month=m,
#                 year=y,
#                 page=p
#             )

#             get_data = True
#             while get_data:
#                 try:
#                     df = pd.read_html(URL, storage_options=HEADERS)[0]
#                     print(f"{y}-{m}:{p}({len(df)})")
#                     if len(df) != 0:
#                         data = pd.concat([data, df])
#                         data.reset_index(drop=True, inplace=True)
#                     get_data = False
#                 except:
#                     time.sleep(3)
#                     r = requests.get(url = URL, headers = HEADERS)
#                     logging.error(f"Status Code: {r.status_code} - {y}-{m}:{p}")
#                     get_data = True
                    
#     data.to_csv(f"output/{y}.csv", index=False)
