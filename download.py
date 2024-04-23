import time
from datetime import date, timedelta
import pandas as pd
from utils import Meteomanz, countdown, dateRange


scale = "hour"

start_date = date(2000, 5, 18)
end_date = date(2023, 12, 31)


if scale == "day":
    for year in [*map(str, range(2000, 2024, 1))]:
        df_year = pd.DataFrame()
        for month in [*map(lambda x: str(x).zfill(2), range(1, 13, 1))]:
            countdown(t=5, start_txt="Waiting for the Next Month!", end_txt="Start Downloading Data...")
            df_month = pd.DataFrame()
            meteo = Meteomanz(
                scale = scale,
                day_start = "01",
                day_end = "31",
                month = month,
                year = year,
            )
            number_of_pages = meteo.pages()
            if number_of_pages > 1:
                for page in list(range(1, number_of_pages + 1)):
                    meteo = Meteomanz(
                        scale = scale,
                        day_start = "01",
                        day_end = "31",
                        month = month,
                        year = year,
                        page=page
                    )
                    df_page = meteo.download()
                    print(meteo.url())
                    if len(df_page) != 0:
                        df_month = pd.concat([df_month, df_page])
                        df_month.reset_index(drop=True, inplace=True)                     
            else:
                df_month = meteo.download()
                print(meteo.url())
            if len(df_month) != 0:
                df_year = pd.concat([df_year, df_month])
                df_year.reset_index(drop=True, inplace=True)
        df_year.to_csv(f"output/{scale}/{year}.csv", index=False)
        
elif scale == "hour":
    for dt in dateRange(start_date=start_date, end_date=end_date):
        countdown(t=5, start_txt="Waiting for the Next Day!", end_txt="Start Downloading Data...")
        year, month, day = dt.strftime("%Y-%m-%d").split("-")
        df_day = pd.DataFrame()
        meteo = Meteomanz(
            scale = scale,
            hour_start="00Z",
            hour_end="23Z",
            day_start = day,
            day_end = day,
            month = month,
            year = year,
        )
        number_of_pages = meteo.pages()
        if number_of_pages > 1:
            for page in list(range(1, number_of_pages + 1)):
                meteo = Meteomanz(
                    scale = scale,
                    hour_start="00Z",
                    hour_end="23Z",
                    day_start = day,
                    day_end = day,
                    month = month,
                    year = year,
                    page=page
                )
                df_page = meteo.download()
                print(meteo.url())
                if len(df_page) != 0:
                    df_day = pd.concat([df_day, df_page])
                    df_day.reset_index(drop=True, inplace=True)
        else:
            df_day = meteo.download()
            print(meteo.url())
        df_day.to_csv(f"output/{scale}/{year}-{month}-{day}.csv", index=False)