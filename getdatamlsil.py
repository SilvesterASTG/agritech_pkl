import json
import requests
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)

# Konfigurasi database
db_config = {
    'user': 'root',
    'password': 'agritech',
    'host': 'localhost',
    'database': 'agritech'
}

# Fungsi untuk mendapatkan daftar nama dari database
def get_device_names(cursor):
    cursor.execute("SELECT nama, stasiun, jenis, tanggalupdate FROM device_data")
    devices = cursor.fetchall()
    return devices

# Fungsi untuk mendapatkan token akses
def get_access_token():
    url = "https://data.mertani.co.id/users/login"
    data = {
        "strategy": "web",
        "email": "reza26pahlevi@gmail.com",
        "password": "divisi02"
    }
    response = requests.post(url, json=data)
    data = json.loads(response.content)
    access_token = data["data"]["accessToken"]
    return access_token

# Fungsi untuk memanggil API dan mendapatkan data kadar air terbesar
def get_max_kadar_air(device_name, start_date_str, end_date_str):
    url = f"http://lebungapi.gg-foods.com/api?startDate={start_date_str}&endDate={end_date_str}&source={device_name}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['response'] == 'OK':
            df = pd.DataFrame(data['data'])
            if 'sensor_id' in df.columns:
                df = df[df['sensor_id'] == 'kadarair']
                if not df.empty:
                    return df['value'].astype(float).max()
    else:
        logging.error(f"Error: {response.status_code}, {response.text}")
    return 0.0

# Fungsi untuk mengunduh dan memperbarui data dari satu stasiun
def update_and_download_data(sensor_company_id, metric, token):
    headers = {"Authorization": f"Bearer {token}"}
    today = datetime.today().date()
    start_date = f"{today} 00:00:00"
    end_date = f"{today} 23:59:59"
    url = "https://data.mertani.co.id/sensors/records"
    params = {
        "sensor_company_id": sensor_company_id,
        "start": start_date,
        "end": end_date
    }
    response = requests.get(url, params=params, headers=headers)
    
    if response.status_code == 200:
        try:
            data = response.json()
            if "data" in data and "data" in data["data"] and len(data["data"]["data"]) > 0:
                records = data["data"]["data"][0]["sensor_records"]
                values = [record["value_calibration"] for record in records]
                return max(values), min(values), sum(values) / len(values)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
            logging.error(f"Response content: {response.content}")
    return 0, 0, 0

# Fungsi untuk mendapatkan nilai dari berbagai sensor
def get_sensor_data(station_name, token):
    station_mapping = {
        "OP1": {
            "airhum": "80b14083-05a9-4d1e-bad0-2da9a45999ef",
            "windspeed": "80b14083-05a9-4d1e-bad0-2da9a45999ez",
            "airtemp": "0bb2fab9-6bde-4af2-b92a-cef27e8e2299",
            "airpress": "525cf684-bba4-46a8-b7a1-85a292d7e2fz"
        },
        "Kijung": {
            "airhum": "b0f0eb3f-7733-45fd-b4db-2162b55d796a",
            "windspeed": "4eb52c7a-5ec6-490c-8ca0-0c8c4fbb0aa5",
            "airtemp": "edd15872-91cd-47bd-9a94-f874fdb2a7e0",
            "airpress": "e9bd6c86-4ae2-40de-84b2-6da2491ee34f"
        },
        "Lakop": {
            "airhum": "cdd78a57-1740-4fa7-b695-66d290783c07",
            "windspeed": "c3f984c8-84dd-4e2c-b1b2-3c8e6aa5f7a9",
            "airtemp": "efb943b8-a1dc-45be-8601-69c0340d5404",
            "airpress": "1bf863f2-2ef2-4967-9643-915f2ba39364"
        },
        "RnD": {
            "airhum": "7675f52b-d75d-43d3-9bc8-6a81340afc8b",
            "windspeed": "1825def9-196b-4a64-bf33-71cda75fb0c5",
            "airtemp": "9ce0f602-bbaf-431c-a45a-e978de5ebb65",
            "airpress": "beecad05-f259-41c0-b752-c3243cdbff64"
        },
        "Divisi4": {
            "airhum": "7b756ef3-8bc5-4a36-a72d-80d21bc4bcce",
            "windspeed": "e5201dc4-685c-4807-8fb7-0155c941ba8c",
            "airtemp": "6d46ac2d-3e26-4111-a0e6-38c89627a51d",
            "airpress": "479fe889-d03e-4157-a0bd-5e09a8522649"
        },
        "OP2": {
            "airhum": "67e294ff-7ba3-4a4d-bd7e-03e44b06f2a8",
            "windspeed": "9107f003-c67e-4992-810b-ca1faafa2520",
            "airtemp": "d03845cb-513c-4157-9fcb-9326c5674ce0",
            "airpress": "b0c04267-9c29-436f-a6e4-9b768cec4460"
        },
        "PG3Central": {
            "airhum": "7d8fd667-0ccd-4f9f-aab8-83765a34271a",
            "windspeed": "7d8fd667-0ccd-4f9f-aab8-83765a34271z",
            "airtemp": "8a32541e-c0d6-438d-9b8e-d892504913ff",
            "airpress": "8cd302ee-70e2-4be6-b9ef-577403249282"
        },
        "Paris": {
            "airhum": "3f713167-4223-490d-b120-edd2f8f7578b",
            "windspeed": "81b5ee0c-d6c8-4d6d-9905-814234d8baa8",
            "airtemp": "3f4b0b89-be65-44f7-a338-cc4fe4b68db6",
            "airpress": "8cd302ee-70e2-4be6-b9ef-577403249282"
        },
        "PG4Central": {
            "airhum": "6b4c8b6f-07db-49a4-b3b7-14e15e2e6ae1",
            "windspeed": "6b4c8b6f-07db-49a4-b3b7-14e15e2e6ae1z",
            "airtemp": "08a26a52-3146-4b93-ae65-f27e25945643",
            "airpress": "08a26a52-3146-4b93-ae65-f27e25945643"
        },
        "PH": {
            "airhum": "e8ee786b-c59d-4e42-a8a6-c1811aa9be61",
            "windspeed": "8c5e7b9a-bf17-4d6d-8f7e-320d7b8b6012",
            "airtemp": "d1a2e8b5-9a7c-4d3b-8c1a-8f5a6e4c3212",
            "airpress": "d1a2e8b5-9a7c-4d3b-8c1a-8f5a6e4c3212"
        },
    }

    results = {}
    if station_name in station_mapping:
        for metric, sensor_company_id in station_mapping[station_name].items():
            max_val, min_val, avg_val = update_and_download_data(sensor_company_id, metric, token)
            results[metric] = (max_val, min_val, avg_val)
    return results

# Fungsi utama untuk mengambil data dari berbagai sumber dan mengirimkan ke API
def main():
    logging.info("Memulai proses...")
    
    # Mendapatkan token akses
    token = get_access_token()
    
    # Membuat koneksi ke database
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        # Mendapatkan nama perangkat dari database
        devices = get_device_names(cursor)
        
        for device in devices:
            device_name, station_name, device_type, last_update = device
            logging.info(f"Memproses {device_name} di stasiun {station_name}")
            
            # Mendapatkan data kadar air dari API
            today = datetime.today().date()
            start_date_str = f"{today} 00:00:00"
            end_date_str = f"{today} 23:59:59"
            max_kadar_air = get_max_kadar_air(device_name, start_date_str, end_date_str)
            logging.info(f"Kadar air tertinggi untuk {device_name}: {max_kadar_air}")

            # Mendapatkan data sensor lainnya
            sensor_data = get_sensor_data(station_name, token)
            logging.info(f"Data sensor untuk {station_name}: {sensor_data}")

            # Siapkan data untuk dikirim ke API prediksi
            prediction_data = {
                "kadar_air": max_kadar_air,
                "humidity_max": sensor_data['airhum'][0],
                "humidity_min": sensor_data['airhum'][1],
                "humidity_avg": sensor_data['airhum'][2],
                "windspeed_max": sensor_data['windspeed'][0],
                "windspeed_min": sensor_data['windspeed'][1],
                "windspeed_avg": sensor_data['windspeed'][2],
                "temperature_max": sensor_data['airtemp'][0],
                "temperature_min": sensor_data['airtemp'][1],
                "temperature_avg": sensor_data['airtemp'][2],
                "pressure_max": sensor_data['airpress'][0],
                "pressure_min": sensor_data['airpress'][1],
                "pressure_avg": sensor_data['airpress'][2],
            }

            # Kirim data ke API prediksi
            pred_url = "http://lebungapi.gg-foods.com/predict"
            pred_response = requests.post(pred_url, json=prediction_data)
            if pred_response.status_code == 200:
                pred_result = pred_response.json()
                logging.info(f"Prediksi untuk {device_name}: {pred_result}")
            else:
                logging.error(f"Gagal mengirim data ke API prediksi untuk {device_name}: {pred_response.status_code}, {pred_response.text}")
    
    except Exception as e:
        logging.error(f"Terjadi kesalahan: {e}")
    
    finally:
        cursor.close()
        connection.close()
        logging.info("Proses selesai")

if __name__ == "__main__":
    main()
