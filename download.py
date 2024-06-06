import sys
from datetime import date, datetime
import pandas as pd
from utils import Meteomanz, date_range, find_start_date, find_end_date, extract_station_code


SCALE = sys.argv[1]

stations = extract_station_code(path_station_txt="stations.csv", code_col_name="CODE")
end_date = find_end_date(scale=SCALE)



if SCALE == "year":
    for st_code in stations:
        df_station = pd.DataFrame()
        start_date = find_start_date(scale=SCALE, station=st_code)
        if start_date >= end_date:
            continue
        meteo = Meteomanz(
            scale="year",
            country_code="2060",
            station_code=str(st_code),
            year_start=str(start_date.year).zfill(4),
            year_end=str(end_date.year).zfill(4),
        )
        number_of_pages = meteo.pages()
        if number_of_pages > 1:
            pass
        else:
            print(meteo.url())
            df_station = meteo.download()
            if df_station.empty:
                continue
            try:
                df = pd.read_csv(f"output/{SCALE}/{st_code}.csv")
                df_station = pd.concat([df, df_station])
            except:
                pass
        df_station.to_csv(f"output/{SCALE}/{st_code}.csv", index=False)

if SCALE == "month":
    for st_code in stations:
        df_station = pd.DataFrame()
        start_date = find_start_date(scale=SCALE, station=st_code)
        if start_date >= end_date:
            continue
        meteo = Meteomanz(
            scale="month",
            country_code="2060",
            station_code=str(st_code),
            month_start=str(start_date.month).zfill(2),
            month_end=str(end_date.month).zfill(2),
            year_start=str(start_date.year).zfill(4),
            year_end=str(end_date.year).zfill(4),
        )
        number_of_pages = meteo.pages()
        if number_of_pages > 1:
            pass
        else:
            print(meteo.url())
            df_station = meteo.download()
            if df_station.empty:
                continue
            df_station.drop(df_station.tail(1).index,inplace=True)
            try:
                df = pd.read_csv(f"output/{SCALE}/{st_code}.csv")
                df_station = pd.concat([df, df_station])
            except:
                pass
        df_station.to_csv(f"output/{SCALE}/{st_code}.csv", index=False)
        
elif SCALE == "day":
    start_date = find_start_date(scale=SCALE)
    if start_date >= end_date:
        print("No data to download")
        dym = pd.date_range(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            freq='MS'
        ).strftime("%Y-%m").tolist()
        
        print(dym)
    else:
        pass
        
    
    
    
    
    
    # for year in [*map(str, range(2024, 2025, 1))]:
    #     df_year = pd.DataFrame()
    #     for month in [*map(lambda x: str(x).zfill(2), range(1, 13, 1))]:
    #         df_month = pd.DataFrame()
    #         meteo = Meteomanz(
    #             scale=SCALE,
    #             day_start="01",
    #             day_end="31",
    #             month=month,
    #             year=year,
    #         )
    #         number_of_pages = meteo.pages()
    #         if number_of_pages > 1:
    #             for page in list(range(1, number_of_pages + 1)):
    #                 meteo = Meteomanz(
    #                     scale=SCALE,
    #                     day_start="01",
    #                     day_end="31",
    #                     month=month,
    #                     year=year,
    #                     page=page
    #                 )
    #                 df_page = meteo.download()
    #                 print(meteo.url())
    #                 if len(df_page) != 0:
    #                     df_month = pd.concat([df_month, df_page])
    #                     df_month.reset_index(drop=True, inplace=True)
    #         else:
    #             df_month = meteo.download()
    #             print(meteo.url())
    #         if len(df_month) != 0:
    #             df_year = pd.concat([df_year, df_month])
    #             df_year.reset_index(drop=True, inplace=True)
    #     df_year.to_csv(f"output/{SCALE}/{year}.csv", index=False)
        
elif SCALE == "hour":
    start_date = find_start_date(scale=SCALE)
    for dt in date_range(start_date=start_date, end_date=end_date):
        year, month, day = dt.strftime("%Y-%m-%d").split("-")
        df_day = pd.DataFrame()
        meteo = Meteomanz(
            scale=SCALE,
            hour_start="00Z",
            hour_end="23Z",
            day_start=day,
            day_end=day,
            month=month,
            year=year,
        )
        number_of_pages = meteo.pages()
        if number_of_pages > 1:
            for page in list(range(1, number_of_pages + 1)):
                meteo = Meteomanz(
                    scale=SCALE,
                    hour_start="00Z",
                    hour_end="23Z",
                    day_start=day,
                    day_end=day,
                    month=month,
                    year=year,
                    page=page
                )
                df_page = meteo.download()
                print(meteo.url())
                if len(df_page) != 0:
                    df_day = pd.concat([df_day, df_page])
                    df_day.reset_index(drop=True, inplace=True)
        else:
            print(meteo.url())
            df_day = meteo.download()
        df_day.to_csv(f"output/{SCALE}/{year}-{month}-{day}.csv", index=False)
