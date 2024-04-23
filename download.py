import time
import pandas as pd
from utils import Meteomanz, countdown


scale = "hour"


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
    for year in [*map(str, range(2000, 2024, 1))]:
        for month in [*map(lambda x: str(x).zfill(2), range(1, 13, 1))]:
            df_month = pd.DataFrame()
            for day in [*map(lambda x: str(x).zfill(2), range(1, 32, 1))]:
                countdown(t=10, start_txt="Waiting for the Next Day!", end_txt="Start Downloading Data...")
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
                if len(df_day) != 0:
                    df_month = pd.concat([df_month, df_day])
                    df_month.reset_index(drop=True, inplace=True)
            df_month.to_csv(f"output/{scale}/{year}-{month}.csv", index=False)