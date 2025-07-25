import json
import os
import ssl
import sys
import time
from urllib.parse import urlparse
import tempfile

import chromedriver_autoinstaller
import pyotp
from kiteconnect import KiteConnect
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from database import REDIS_DB_CLIENT

ssl._create_default_https_context = ssl._create_unverified_context




def run_zerodha_login(api_name, api_key, secret_key, client_id, user_password, totp_key, redirect_url, is_headless=True):
    try:
        options = Options()
        if is_headless:
            options.add_argument("--headless")
        # options.binary_location = chrome_path
        options.add_argument("--disable-gpu")
        options.add_argument("--log-level=3")
        options.add_argument("--silent")
        options.add_argument("--disable-logging")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")

        # ✅ Create unique user-data-dir AND specify remote-debugging-port
        user_data_dir = tempfile.mkdtemp()
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--remote-debugging-port=9222")  # Needed for profile isolation
        chromedriver_autoinstaller.install()
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"ExecutorWorker error: {e}")
        return {
            "status": False,
            "message": f"Failed to set up Chrome driver {e}"
        }
    try:
        driver.maximize_window()
        url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}&redirect_params=account_id%3D{client_id}"

        driver.get(url)
        user = WebDriverWait(driver, 10).until(
            lambda driver: driver.find_element(by="id", value="userid"))
        user.send_keys(client_id)
        password = WebDriverWait(driver, 10).until(
            lambda driver: driver.find_element(by="id", value="password"))
        password.send_keys(user_password)
        login_btn = WebDriverWait(driver, 10).until(
            lambda driver: driver.find_element(by="class name", value="button-orange"))
        login_btn.click()
        authkey = pyotp.TOTP(totp_key)
        totp_form = WebDriverWait(driver, 10).until(
            lambda driver: driver.find_element(by="xpath", value='//input[@label="External TOTP"]'))
        totp_form.send_keys(authkey.now())
        if hasattr(sys, 'frozen'):
            import win32con
            import win32gui

            win = win32gui.GetForegroundWindow()
            win32gui.ShowWindow(win, win32con.SW_HIDE)
            sys.stdout = open(os.devnull, 'w')
            sys.stderr = open(os.devnull, 'w')

        try:
            print("getting params")
            while True:
                parsed_url = urlparse(driver.current_url)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                if base_url != redirect_url:
                    print("Current URL is not the expected redirect URL, retrying...")
                    print("Current URL is " + base_url)
                    print("Redirect URL is " + redirect_url)
                    time.sleep(3)
                    continue
                else:
                    print("Current URL is the expected redirect URL, proceeding...")
                    break

            params = driver.current_url.split("?")[1]
            print(f"URL: {driver.current_url}\nParams: {params}")
            params = params.split("&")
            params = {param.split("=")[0]: param.split("=")[1]
                      for param in params}
        except Exception as e:
            print(f"Some Error Occured Please Try Again\n{e}")
            driver.close()
            return None
        if "request_token" not in params:
            driver.close()
        request_token = params.get("request_token")
        driver.close()
        kite = KiteConnect(api_key=api_key)
        request_token = request_token

        rec_data = kite.generate_session(request_token, api_secret=secret_key)
        access_token = rec_data["access_token"]
        data_to_save = {
            "api_name": api_name,
            "access_token": access_token,
            "api_key": api_key,
            "secret_key": secret_key,
            "client_id": client_id,
        }
        print("Zerodha login successful")
        REDIS_DB_CLIENT.hset("ZERODHA_TOKENS_FOR_MINUTE_DATA", api_name, json.dumps(data_to_save))
        REDIS_DB_CLIENT.expire("ZERODHA_TOKENS_FOR_MINUTE_DATA", 60 * 60 * 15)
    except Exception as e:
        print(f"ExecutorWorker error: {e}")
        return {
            "status": False,
            "message": f"Failed to set up Chrome driver {e}"
        }
    finally:
        driver.quit()
    return None


if __name__ == "__main__":
    accounts = [
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS1",
            "api_key": "g98eqgqqar520f3q",
            "secret_key": "z1atd697szhmo4jzq287nwuesc8w6iyd",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS2",
            "api_key": "lt1ggmxze1c5yc7h",
            "secret_key": "v5i33xz1y1030sk3dgmsw1wuvv6hyt71",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS3",
            "api_key": "6kljxy4h5ykxls69",
            "secret_key": "w8lp59woc979x2mf7g5ajadvexldf7sa",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS4",
            "api_key": "jnkvk8nct8s0mgs2",
            "secret_key": "ld2ve91jjq5hz5kx3bvpdsr8d77ok2s5",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS5",
            "api_key": "jpqfxfpn9xwk89hc",
            "secret_key": "6h4cndasfbkdomnjf7yqqhudh6aawwfd",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS6",
            "api_key": "mmtojcpd93xcbw2o",
            "secret_key": "6sormf7b13q8d2qr2v9klmxuxjnx80sk",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS7",
            "api_key": "o15kzfd2d3mw299w",
            "secret_key": "q0bte7n96b3fkjo6wn695016tl8t4v68",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS8",
            "api_key": "7rgqhuy4jy7ay3le",
            "secret_key": "ddd5m3yu14y8e1z2l0lladobwj4n04of",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS9",
            "api_key": "l30m1qkkfqylzuj2",
            "secret_key": "1n93mqpvikx1adjyc1eykn9luk5ra1wk",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS10",
            "api_key": "ymzzw0gew0fsoek0",
            "secret_key": "6cf6lwgukf25q7pjwnifyzrtn3641dm1",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS11",
            "api_key": "6t5ttuvmaz8jx8r3",
            "secret_key": "sy3evfozi2uf2dkqkfsh4gbro53y3ee4",
        },
        {
            "client_id": "ZGN479",
            "user_password": "Chelsea@pratik7",
            "totp_key": "NV4GMVI2TJNTE5RYNHVDQ7UROQGCZ3WE",
            "redirect_url": "https://www.google.com",

            "name": "TS12",
            "api_key": "4fr9u2q8v6drbfsl",
            "secret_key": "ckb79kiv5abzo770vq2u9axvdtrhpdg1",
        },
    ]

    for acc in accounts:
        print("Running Zerodha login for account:", acc['name'])
        run_zerodha_login(
            acc['name'],
            acc["api_key"],
            acc["secret_key"],
            acc["client_id"],
            acc["user_password"],
            acc["totp_key"],
            acc["redirect_url"],
            True
        )
