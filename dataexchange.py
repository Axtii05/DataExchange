import psycopg2
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
import base64
import logging
import binascii
import datetime
from io import BytesIO
from decimal import Decimal
import schedule
import time


credentials_info = {
    "type": "service_account",
    "project_id": "hip-watch-435010-h2",
    "private_key_id": "70f1033fa03d659b6d98d9d5e314853a0d77bb81",
    "private_key": """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC4RHYwJMUJv4MZ\ntfcx38OwXQ3GNjfJpNlZJ4PRuCC8ya4uNRbBLDHs+xIbDLNHe7JywUUfZSInrAUn\nVTwUpjfngJfWcsaeloT+x2sge9+i1JP8Wbgcy/MBvUaLHFOuajU9jSJMhw/b30hx\nTUfJFtveu6jGq7KOOt+urWx6rPaB6a11qPe7KFuAzDN6uY6JwsQsDi4yBLHZ0gHc\nXIQ4b/BrY0V5NWlXVUj8AKG8VyAS4wCAhmO5qAFdEf2+/CYxcY7qDhZhzkvaAUbp\nwXsgmeVI+1L8O8tXkQfjDtoKX+t+MhqK6KVV06MSDMvvxNO740I0xPaiGOdLGJgJ\nZjcA906NAgMBAAECggEAITM0mDgHyFnZJSjEARhwCba6ZJwgMRSilI2qEn86Zslv\nuKOYFfYAiNIz0OvY4WhIHqTz71QpczxrMUsKKC5KpEtnEBxd7PTxwXXst02aXZwv\njagcpBObFRUlQKs56JL6RLzJEUDEerkgNEnYRUL5Goh1QFZvNXEmHDLi+LdxMra4\npLV5gcqhjAyYnjvZMrt081itZrr3NmWivlTu2N0VHGn5uuNIeG4kxWDbFzXcuoAD\nApAzWFHRXPDGwM52Lz8+rcvzW/z3oLbYUx7wzPNryNKFO7gudRrVX8wj5AuUnLLT\nSmhZKR8X+sohWMIQUJCSmSpLFCIj0Kd/QRd3Va9XgQKBgQD5ijfGQmPAvrJQsWnS\n+8gU4WDaVv/YXzeJ9LFL1CTriwCDaWmCIL9KuiWQPpfUqhbiNDd6XHFDrEI5Ju6m\nwgtDdFmE9YLkRh8rjAuws5iaGGA9f2FHdweXCd6jkWn84kIAdtnRWwJqzMH+SgC3\nQgshARiNRDULmJx6TvZQcNctVwKBgQC9CaltUzcA8aoWlYdrdI5KX+Ns2A/+kN9M\nzi4Od3W6yNFH2nDyaAv+hxxFshsiH1QmkUS5Tzn/+Oq+qaF3KN1SHV3Sdwp1gNZ2\nuiulZfnR3LJeBkaFq5vyAWTb7pDNIPrbAWu3KBR9x6y1TZVuoV6m40OPjtxmvKVl\nsZ6MFpJQuwKBgE2Nt2fWmkn8+k451TnNJpHWudMh1sHYVdp6Qd/fPto1iSNOT4wx\nwUHoOGRsD6P4eMQ0lklhEZKGps48W09YX0fHkUrRQqzPXkCadcelCXhauw5h2Ent\npF48owUS3G3Lo9ehGHEIZ+fpWyE8vpw12l+Xh5nf6NyBwaABimvVpgphAoGAbIzc\nxpNvVVJwSjGpJpIylxDC5qCXZqXIJDGNDu+YIh7o1irgisImiQ0KthbVL93vk4n9\nfa+57XwBSGTd/C/yDxIf6xhCYEEQZfL31y1crB7gKc/OtTla/jfAs+4lJjWW9yW0\nteMFCUkcqquXcISndouwIwJ1G2WeUGwTT/wyHrsCgYAhrBYDO6mMbDDlHzpmNT0Q\nLb8wr4xLAUI47PU+x+OCWICAo4xeoL44AHUod5j4O35IQ+sZLH4R7+cuaBvlrvQl\ndUVriL3MViyI4wGmAhsiOTBDuw5SceL9DrJDMhsifo4gHSUTbWK5s1A4ys6ivTPJ\nv7bdD/z04udtneyGn7CsqQ==\n-----END PRIVATE KEY-----\n""",
    "client_email": "formkplacebot@hip-watch-435010-h2.iam.gserviceaccount.com",
    "client_id": "110580102490115208145",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/formkplacebot%40hip-watch-435010-h2.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_postgres_data():
    try:
        # Подключение к PostgreSQL
        conn = psycopg2.connect(
            database="postgres",
            user="postgres.zywjihrcgdozorytmbhy",
            password="Karypb@ev05",
            host="aws-0-eu-central-1.pooler.supabase.com",
            port="6543"
        )
        cursor = conn.cursor()

        # Получение данных, фото получаем в формате bytea
        query = """
        SELECT 
            u.telegram_username,
            u.phone_number,
            u.is_paid,
            r.warehouses,
            r.delivery_type,
            r.reception_type,
            r.request_date,
            r.coefficient,
            r.photo  -- Получаем фото в формате bytea
        FROM 
            users u
        JOIN 
            requests r 
        ON 
            u.user_id = r.user_id;
        """
        cursor.execute(query)
        data = cursor.fetchall()

        data = [(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], bytes(row[8]) if row[8] is not None else None) for row in data]
        # Закрытие соединения с PostgreSQL
        cursor.close()
        conn.close()
        logging.info("Данные успешно получены из PostgreSQL")
        return data

    except Exception as e:
        logging.error(f"Ошибка при работе с PostgreSQL: {e}")
        return []

def authenticate_google_sheets():
    try:
        # Аутентификация в Google Sheets API
        SCOPES = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                  "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]


        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES)

        service = build('sheets', 'v4', credentials=credentials)
        logging.info("Аутентификация в Google Sheets прошла успешно")
        return service

    except Exception as e:
        logging.error(f"Ошибка при аутентификации в Google Sheets: {e}")
        return None

def upload_to_google_drive(service, bytea_data, mime_type):
    """Функция для загрузки изображения на Google Drive и получения ссылки на него"""
    try:
        file_metadata = {'name': 'image', 'mimeType': mime_type}
        media = MediaIoBaseUpload(BytesIO(bytea_data), mimetype=mime_type)

        # Создание файла и получение ссылки
        file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        file_id = file.get('id')

        # Изменение настроек доступа, чтобы сделать файл общедоступным
        permission = {'type': 'anyone', 'role': 'reader'}
        service.permissions().create(fileId=file_id, body=permission).execute()

        return file.get('webViewLink')

    except Exception as e:
        logging.error(f"Ошибка при загрузке изображения на Google Drive: {e}")
        return None

def guess_mime_type(bytea_data):
    """
    Функция для определения MIME-типа по первым байтам
    """
    try:
        # Проверим, есть ли вообще данные, прежде чем работать с ними
        if bytea_data is None:
            logging.warning("Получены пустые данные для определения MIME-типа")
            return None

        # Проверяем байтовые данные на сигнатуру файла
        if bytea_data.startswith(b'\xff\xd8'):  # JPEG
            return 'image/jpeg'
        elif bytea_data.startswith(b'\x89PNG'):  # PNG
            return 'image/png'
        elif bytea_data.startswith(b'%PDF'):  # PDF
            return 'application/pdf'
        else:
            logging.warning("Не удалось точно определить MIME-тип")
            return None
    except Exception as e:
        logging.error(f"Ошибка при определении MIME-типа: {e}")
        return None


def determine_image_formula(service, bytea_data):
    """Функция для получения ссылки на изображение, загруженное на Google Drive"""
    if bytea_data is None:
        return 'Нет изображения'

    mime_type = guess_mime_type(bytea_data)
    image_link = upload_to_google_drive(service, bytea_data, mime_type)

    if image_link:
        return image_link
    else:
        return 'Ошибка загрузки изображения'

def update_google_sheets(sheets_service, drive_service, data, spreadsheet_id, range_name):
    try:
        sheet = sheets_service.spreadsheets()

        # Подготовка данных для отправки, включая ссылки на изображения
        values = []
        for row in data:
            bytea_data = row[8]  # Столбец с двоичными данными изображения
            photo_formula = determine_image_formula(drive_service, bytea_data)  # Передаем drive_service
            values.append(list(row[:8]) + [photo_formula])

        # Преобразование дат и Decimal в строки
        for i, row in enumerate(values):
            for j, value in enumerate(row):
                if isinstance(value, datetime.date):
                    values[i][j] = value.strftime('%Y-%m-%d')
                elif isinstance(value, Decimal):
                    values[i][j] = str(value)

        body = {
            'values': values
        }

        result = sheet.values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption='USER_ENTERED', body=body).execute() 


        logging.info(f"{result.get('updatedCells')} ячеек обновлено.")
        return result.get('updatedCells')

    except Exception as e:
        logging.error(f"Ошибка при обновлении данных в Google Sheets: {e}")
        return 0

def main():
    # Получение данных из PostgreSQL
    data = get_postgres_data()

    # Аутентификация и работа с Google Sheets
    sheets_service = authenticate_google_sheets()
    if sheets_service and data:
        drive_service = build('drive', 'v3', credentials=sheets_service._http.credentials)
        # Обновление Google Sheets
        spreadsheet_id = '1JZY4-hRMC0SmSpBw8evKch0ggoSJIF0c-du_Fb9SXEI'
        range_name = 'Лист1!A1'  # Укажи нужный лист и диапазон
        updated_cells = update_google_sheets(sheets_service, drive_service, data, spreadsheet_id, range_name)
        logging.info(f"Обновление завершено. Всего обновлено ячеек: {updated_cells}")

        # Планирование обновления каждые полчаса
        schedule.every(10).minutes.do(update_google_sheets, sheets_service, drive_service, data, spreadsheet_id, range_name)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        logging.error("Обновление Google Sheets не выполнено")

if __name__ == '__main__':
    main()