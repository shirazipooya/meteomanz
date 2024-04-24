import os
import math
import re
import datetime
import logging
import time
import sys
import pandas as pd
import requests
from dotenv import load_dotenv
from pushbullet import Pushbullet


# # Pushbullet
# load_dotenv()
# API_KEY = os.environ["PUSHBULLET_ACCESS_TOKEN"]
# pb = Pushbullet(API_KEY)
# push = pb.push_note("Meteomanz", "The Meteomanz Script is Running ...!")


# Proxies
SET_PROXY = False

proxies = {
    'http': 'socks5://98.181.137.80:4145',
    'https': 'socks5://98.181.137.80:4145'
}


logging.basicConfig(
    level=logging.ERROR,
    filename='app.log',
    filemode='a',
    format='%(message)s'
)


def find_start_date(scale):
    if scale == "hour":
        lf = os.listdir(f"output/{scale}/")
        date_max = max(list(filter(lambda f: f.endswith('.csv'), lf)))[:-4].split("-")
        return datetime.date(int(date_max[0]), int(date_max[1]), int(date_max[2]))


def countdown(
    t=60,
    start_txt="Error: Connection Timed Out!",
    end_txt="Try Again ...!"
):
    """Generate Countdown Timer

    Args:
        t (int, optional): The duration of the countdown in seconds. Defaults to 60.
        start_txt (str, optional): The text to display at the start of the countdown. Defaults to "Error: Connection Timed Out!".
        end_txt (str, optional): The text to display at the end of the countdown. Defaults to "Try Again ...!".
    """
    print("-" * 100)
    while t >= 0:
        sys.stdout.write(f"\r{start_txt} ({t} Seconds Remaining ...!)")
        t -= 1
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write(f"\r\n{end_txt}            \n")
    print("-" * 100)


def date_range(start_date, end_date):
    """
    Generate a range of dates between the start_date and end_date (exclusive).

    Args:
        start_date (datetime.date): The start date of the range.
        end_date (datetime.date): The end date of the range.

    Yields:
        datetime.date: The dates in the range.

    Example:
        >>> start_date = datetime.date(2022, 1, 1)
        >>> end_date = datetime.date(2022, 1, 5)
        >>> for date in date_range(start_date, end_date):
        ...     print(date)
        ...
        2022-01-01
        2022-01-02
        2022-01-03
        2022-01-04
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


class Meteomanz():
    """
    A class representing the Meteomanz weather data.

    Attributes:
        scale (str): The time scale of the weather data (e.g., "hour" or "day").
        country_code (str): The country code for the weather data.
        station_code (str): The station code for the weather data.
        hour_start (str): The start hour for the weather data.
        hour_end (str): The end hour for the weather data.
        day_start (str): The start day for the weather data.
        day_end (str): The end day for the weather data.
        month (str): The month for the weather data.
        year (str): The year for the weather data.
        page (str): The page number for the weather data.
        userAgent (str): The user agent for the HTTP request.
        referer (str): The referer for the HTTP request.
        host (str): The host for the HTTP request.
        acceptLanguage (str): The accept language for the HTTP request.
        connection (str): The connection type for the HTTP request.
        accept (str): The accept type for the HTTP request.
    """

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
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        referer="http://www.meteomanz.com/",
        host="www.meteomanz.com",
        accept_language="en-US,en;q=0.9",
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
        self.user_agent = user_agent
        self.referer = referer
        self.host = host
        self.accept_language = accept_language
        self.connection = connection
        self.accept = accept

    def url(self):
        """
        Generates the URL based on the scale and other attributes.

        Returns:
            str: The generated URL.
        """
        if self.scale == "hour":
            return f"http://www.meteomanz.com/sy1?ty=hp&l=1&cou={self.country_code}&ind={self.station_code}&d1={self.day_start}&m1={self.month}&y1={self.year}&h1={self.hour_start}&d2={self.day_end}&m2={self.month}&y2={self.year}&h2={self.hour_end}&so=001&np={self.page}"
        elif self.scale == "day":
            return f"http://www.meteomanz.com/sy2?ty=hp&l=1&cou={self.country_code}&ind={self.station_code}&d1={self.day_start}&m1={self.month}&y1={self.year}&d2={self.day_end}&m2={self.month}&y2={self.year}&so=001&np={self.page}"

    def header(self):
        """
        Generates the header for the HTTP request.

        Returns:
            dict: The generated header.
        """
        return {
            "User-Agent": self.user_agent,
            "Referer": self.referer,
            "Host": self.host,
            "Accept-Language": self.accept_language,
            "Connection": self.connection,
            "Accept": self.accept
        }

    def pages(self):
        """
        Retrieves the number of pages for the weather data.

        Returns:
            int: The number of pages.
        """
        while True:
            if SET_PROXY:
                r = requests.get(
                    url=self.url(),
                    headers=self.header(),
                    timeout=20,
                    proxies=proxies
                )
            else:
                r = requests.get(
                    url=self.url(),
                    headers=self.header(),
                    timeout=20
                )
            if r.status_code == 200:
                try:
                    if SET_PROXY:
                        html_content = requests.get(
                            self.url(),
                            headers=self.header(),
                            timeout=20,
                            proxies=proxies
                        ).content
                    else:
                        html_content = requests.get(
                            self.url(),
                            headers=self.header(),
                            timeout=20,
                        ).content
                    txt = [value for value in html_content.split(b"\n") if (value.lower().__contains__(
                        b'showing') and value.lower().__contains__(b'results'))][0].decode('utf-8')
                    num = [int(num)
                           for num in re.findall(r'\d+(?:\.\d+)?', txt)]
                    return math.ceil(num[-1] / num[-2])
                except:
                    return 1
            else:
                countdown(t=60)
                if self.scale == "hour":
                    logging.error(f"Error Pages: {
                                  self.year}-{self.month}-{self.day_start}, Page {self.page}")
                elif self.scale == "day":
                    logging.error(f"Error Pages: {
                                  self.year}-{self.month}, Page {self.page}")

    def download(self):
        """
        Downloads the weather data.

        Returns:
            pandas.DataFrame: The downloaded weather data.
        """
        while True:
            if SET_PROXY:
                r = requests.get(
                    url=self.url(),
                    headers=self.header(),
                    timeout=20,
                    proxies=proxies
                )
            else:
                r = requests.get(
                    url=self.url(),
                    headers=self.header(),
                    timeout=20
                )
            if r.status_code == 200:
                while True:
                    try:
                        if SET_PROXY:
                            session = requests.Session()
                            session.proxies = proxies
                            resp = session.get(
                                self.url(),
                                headers=self.header(),
                                verify=False
                            )
                            df = pd.read_html(
                                resp.text
                            )
                        else:
                            df = pd.read_html(
                                self.url(),
                                storage_options=self.header()
                            )[0]
                        break
                    except:
                        countdown(t=60)
                if self.scale == "hour":
                    print(f"Downloaded {
                          self.year}-{self.month}-{self.day_start}, Page {self.page}")
                elif self.scale == "day":
                    print(f"Downloaded {
                          self.year}-{self.month}, Page {self.page}")
                return df
            else:
                countdown(t=60)
                if self.scale == "hour":
                    logging.error(f"Error Download: {
                                  self.year}-{self.month}-{self.day_start}, Page {self.page}")
                elif self.scale == "day":
                    logging.error(f"Error Download: {
                                  self.year}-{self.month}, Page {self.page}")


class Gregorian:

    def __init__(self, *date):
        if len(date) == 1:
            date = date[0]
            if type(date) is str:
                m = re.match(r'^(\d{4})\D(\d{1,2})\D(\d{1,2})$', date)
                if m:
                    [year, month, day] = [int(m.group(1)), int(
                        m.group(2)), int(m.group(3))]
                else:
                    raise Exception("Invalid Input String")
            elif type(date) is datetime.date:
                [year, month, day] = [date.year, date.month, date.day]
            elif type(date) is tuple:
                year, month, day = date
                year = int(year)
                month = int(month)
                day = int(day)
            else:
                raise Exception("Invalid Input Type")
        elif len(date) == 3:
            year = int(date[0])
            month = int(date[1])
            day = int(date[2])
        else:
            raise Exception("Invalid Input")

        try:
            datetime.datetime(year, month, day)
        except:
            raise Exception("Invalid Date")

        self.gregorian_year = year
        self.gregorian_month = month
        self.gregorian_day = day

        d_4 = year % 4
        g_a = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
        doy_g = g_a[month] + day
        if d_4 == 0 and month > 2:
            doy_g += 1
        d_33 = int(((year - 16) % 132) * .0305)
        a = 286 if (d_33 == 3 or d_33 < (d_4 - 1) or d_4 == 0) else 287
        if (d_33 == 1 or d_33 == 2) and (d_33 == d_4 or d_4 == 1):
            b = 78
        else:
            b = 80 if (d_33 == 3 and d_4 == 0) else 79
        if int((year - 10) / 63) == 30:
            a -= 1
            b += 1
        if doy_g > b:
            jy = year - 621
            doy_j = doy_g - b
        else:
            jy = year - 622
            doy_j = doy_g + a
        if doy_j < 187:
            jm = int((doy_j - 1) / 31)
            jd = doy_j - (31 * jm)
            jm += 1
        else:
            jm = int((doy_j - 187) / 30)
            jd = doy_j - 186 - (jm * 30)
            jm += 7
        self.persian_year = jy
        self.persian_month = jm
        self.persian_day = jd

    def persian_tuple(self):
        return self.persian_year, self.persian_month, self.persian_day

    def persian_string(self, date_format="{}-{}-{}"):
        return date_format.format(self.persian_year, self.persian_month, self.persian_day)


class Persian:

    def __init__(self, *date):
        if len(date) == 1:
            date = date[0]
            if type(date) is str:
                m = re.match(r'^(\d{4})\D(\d{1,2})\D(\d{1,2})$', date)
                if m:
                    [year, month, day] = [int(m.group(1)), int(
                        m.group(2)), int(m.group(3))]
                else:
                    raise Exception("Invalid Input String")
            elif type(date) is tuple:
                year, month, day = date
                year = int(year)
                month = int(month)
                day = int(day)
            else:
                raise Exception("Invalid Input Type")
        elif len(date) == 3:
            year = int(date[0])
            month = int(date[1])
            day = int(date[2])
        else:
            raise Exception("Invalid Input")

        if year < 1 or month < 1 or month > 12 or day < 1 or day > 31 or (month > 6 and day == 31):
            raise Exception("Incorrect Date")

        self.persian_year = year
        self.persian_month = month
        self.persian_day = day

        d_4 = (year + 1) % 4
        if month < 7:
            doy_j = ((month - 1) * 31) + day
        else:
            doy_j = ((month - 7) * 30) + day + 186
        d_33 = int(((year - 55) % 132) * .0305)
        a = 287 if (d_33 != 3 and d_4 <= d_33) else 286
        if (d_33 == 1 or d_33 == 2) and (d_33 == d_4 or d_4 == 1):
            b = 78
        else:
            b = 80 if (d_33 == 3 and d_4 == 0) else 79
        if int((year - 19) / 63) == 20:
            a -= 1
            b += 1
        if doy_j <= a:
            gy = year + 621
            gd = doy_j + b
        else:
            gy = year + 622
            gd = doy_j - a
        for gm, v in enumerate([0, 31, 29 if (gy % 4 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]):
            if gd <= v:
                break
            gd -= v

        self.gregorian_year = gy
        self.gregorian_month = gm
        self.gregorian_day = gd

    def gregorian_tuple(self):
        return self.gregorian_year, self.gregorian_month, self.gregorian_day

    def gregorian_string(self, date_format="{}-{}-{}"):
        return date_format.format(self.gregorian_year, self.gregorian_month, self.gregorian_day)

    def gregorian_datetime(self):
        return datetime.date(self.gregorian_year, self.gregorian_month, self.gregorian_day)
