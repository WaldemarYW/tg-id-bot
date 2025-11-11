import os
import sys
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

def send(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": ADMIN_ID, "text": text}
    try:
        requests.post(url, data=data, timeout=10)
    except Exception:
        pass

if __name__ == "__main__":
    service = sys.argv[1] if len(sys.argv) > 1 else "tg-bot"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    send(f"❌ Сервіс {service} впав о {now}. Перевір логи: journalctl -u tg-bot -xe")
