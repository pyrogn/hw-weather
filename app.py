import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import aiohttp
import asyncio
import requests
from datetime import datetime
import time

WEATHER_API_URL = "http://api.openweathermap.org/data/2.5/weather"
st.set_page_config(page_title="Дашборд анализа погоды", layout="wide")


# загрузка исторических данных
@st.cache_data
def load_data(uploaded_file):
    df = pd.read_csv(uploaded_file)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def get_current_season():
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "fall"


# нормальная ли температура в данном сезоне исходя из статистики этого города
def check_temperature_normality(temp, city_stats):
    season = get_current_season()
    stats = city_stats[city_stats["season"] == season].iloc[0]
    mean, std = stats["mean"], stats["std"]
    is_normal = (mean - 2 * std) <= temp <= (mean + 2 * std)
    return is_normal, mean, std


# синхронный вызов
def get_weather_sync(city, api_key):
    params = {"q": city, "appid": api_key, "units": "metric"}

    response = requests.get(WEATHER_API_URL, params=params)
    data = response.json()
    if response.status_code != 200:
        st.error(data)
        return None

    data = response.json()
    return data["main"]["temp"]


# асинхронный вызов
async def get_weather_async(city, api_key):
    params = {"q": city, "appid": api_key, "units": "metric"}

    async with aiohttp.ClientSession() as session:
        async with session.get(WEATHER_API_URL, params=params) as response:
            data = await response.json()

            if response.status != 200:
                st.error(data)
                return None

            return data["main"]["temp"]


def main():
    st.title("Дашборд анализа погоды")

    with st.sidebar:
        uploaded_file = st.file_uploader("Загрузить исторические данные", type=["csv"])
        api_key = st.text_input("Введите OpenWeatherMap API Key", type="password")
        use_async = st.checkbox("Использовать асинхронный вызов API", value=True)

    # показываем что-либо только после загрузки исторических данных
    if uploaded_file is not None:
        df = load_data(uploaded_file)

        cities = sorted(df["city"].unique())
        selected_city = st.selectbox("Выберите город", cities)

        city_data = df[df["city"] == selected_city]

        city_stats = (
            city_data.groupby("season")["temperature"]
            .agg(["mean", "std"])
            .reset_index()
        )

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Анализ исторической температуры")

            mean = city_data.groupby("season")["temperature"].transform("mean")
            std = city_data.groupby("season")["temperature"].transform("std")
            anomalies = (city_data["temperature"] < (mean - 2 * std)) | (
                city_data["temperature"] > (mean + 2 * std)
            )

            fig = go.Figure()

            fig.add_trace(
                go.Scatter(
                    x=city_data[~anomalies]["timestamp"],
                    y=city_data[~anomalies]["temperature"],
                    mode="lines",
                    name="Нормальная температура",
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=city_data[anomalies]["timestamp"],
                    y=city_data[anomalies]["temperature"],
                    mode="markers",
                    name="Аномалии",
                    marker={"color": "red", "size": 8},
                )
            )

            fig.update_layout(
                title=f"Температура {selected_city}",
                xaxis_title="Дата",
                yaxis_title="Температура (°C)",
            )
            st.plotly_chart(fig)

        with col2:
            st.subheader("Сезонная статистика")

            fig = px.box(
                city_data,
                x="Сезон",
                y="Температура",
                title=f"Температура по сезонам в {selected_city}",
            )
            st.plotly_chart(fig)

            st.write("Сезонная статистика:")
            st.dataframe(city_stats)

        st.subheader("Текущая погода")
        if api_key:
            try:
                start_time = time.time()

                # вообще без разницы, что использовать, так как кажется, что мы не нагружаем систему
                # множеством вызовов
                if use_async:
                    current_temp = asyncio.run(
                        get_weather_async(selected_city, api_key)
                    )
                else:
                    current_temp = get_weather_sync(selected_city, api_key)

                if current_temp is not None:
                    execution_time = time.time() - start_time
                    st.write(
                        f"Текущая температура в {selected_city}: {current_temp:.1f}°C"
                    )
                    st.write(f"Время вызова API: {execution_time:.3f} секунд")

                    is_normal, mean, std = check_temperature_normality(
                        current_temp, city_stats
                    )
                    status = "нормальная" if is_normal else "аномальная"
                    st.write(f"Текущая температура в данном сезоне **{status}**")
                    st.write(
                        f"Средняя температура в сезоне: {mean:.1f}°C ± {2*std:.1f}°C"
                    )

            except Exception as e:
                st.error(f"Ошибка получения температуры: {str(e)}")
        else:
            st.info("Для получения текущей температуры введите API ключ")


if __name__ == "__main__":
    main()
