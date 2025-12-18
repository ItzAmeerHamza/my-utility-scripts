from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager
import time
import csv  # Added for logging

# --- Configuration ---
START_URL = "https://member.recenterhub.com/"
MY_USERNAME = "YOUR_REAL_USERNAME"
MY_PASSWORD = "YOUR_REAL_PASSWORD"
CSV_FILENAME = "property_log.csv"

# --- Selectors ---
# Phase 1: Login
LANDING_LOGIN_BTN = (By.CSS_SELECTOR, "button.cta-button")
USERNAME_INPUT = (By.ID, "Username")
PASSWORD_INPUT = (By.ID, "elPasswordText")
SUBMIT_LOGIN_BTN = (By.CSS_SELECTOR, "button[value='login']")

# Phase 2: Matrix Dashboard
MATRIX_APP_BTN = (By.XPATH, "//div[contains(@class, 'app-card-mls') and .//div[contains(text(), 'Matrix')]]")

# Phase 4: Identity Conflict
IDENTITY_CONTINUE_XPATH = "/html/body/form/div[4]/table/tbody/tr/td/table/tbody/tr/td[3]/a"

# Phase 5: CRMLS Alerts
FIRST_READ_LATER_BTN = (By.ID, "NewsDetailPostpone")

# Phase 6: Links
NAV_LINKS_BTN = (By.XPATH, "//span[contains(text(), 'Links')]/parent::a")
NWMLS_ICON_BTN = (By.CSS_SELECTOR, "a[title='NWMLS']")

# Phase 8: NWMLS Alerts
SECOND_READ_LATER_BTN = (By.XPATH, "//button[contains(., 'Read Later')]")

# Phase 9: Search Menu
GENERAL_SEARCH_LINK = (By.XPATH, "//a[contains(@href, '/Matrix/Search/SingleFamily/GeneralIntraMatrix')]")

# Phase 10: Price Input
PRICE_INPUT = (By.CSS_SELECTOR, "input[data-mtx-track-prop-type='PriceTextBox']")

# Phase 11: Sold Status
SOLD_CHECKBOX = (By.CSS_SELECTOR, "input[data-mtx-track='Status (IntraMatrix) - Sold']")
SOLD_DATE_INPUT = (By.XPATH, "//input[@type='text' and contains(@name, '7421')]")

# Phase 12: Sub Type Selection
SUB_TYPE_SELECT = (By.CSS_SELECTOR, "select[data-mtx-track='Property Sub Type']")

# Phase 13: Search Button
SEARCH_BTN = (By.CSS_SELECTOR, "a[data-mtx-track-webforms-submit='Results - Results Button']")

# Phase 14: Results Table
# Finds all rows in the results grid
RESULT_ROWS = (By.CSS_SELECTOR, "tr.DisplayRegRow")
# Finds the specific MLS link inside a row using the data tracking attribute
MLS_LINK_SELECTOR = (By.CSS_SELECTOR, "a[data-mtx-track='Results - In-Display Full Link Click']")


def dismiss_all_alerts(driver, button_locator, context_name="Alert"):
    print(f"Checking for {context_name}s...")
    iteration = 0
    max_attempts = 10 
    while iteration < max_attempts:
        try:
            wait_short = WebDriverWait(driver, 3)
            btn = wait_short.until(EC.element_to_be_clickable(button_locator))
            try:
                btn.click()
            except ElementClickInterceptedException:
                driver.execute_script("arguments[0].click();", btn)
            print(f"  -> Clicked 'Read Later' ({iteration + 1})")
            time.sleep(1.5)
            iteration += 1
        except (TimeoutException, StaleElementReferenceException):
            print(f"  -> No more {context_name}s found.")
            break

def run_scraper():
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 15)

    try:
        # --- PHASE 1: LOGIN ---
        print(f"Loading: {START_URL}")
        driver.get(START_URL)
        wait.until(EC.element_to_be_clickable(LANDING_LOGIN_BTN)).click()

        print("Entering credentials...")
        wait.until(EC.presence_of_element_located(USERNAME_INPUT)).send_keys(MY_USERNAME)
        driver.find_element(*PASSWORD_INPUT).send_keys(MY_PASSWORD)
        driver.find_element(*SUBMIT_LOGIN_BTN).click()

        # --- PHASE 2: OPEN MATRIX ---
        print("Opening Matrix...")
        original_window = driver.current_window_handle
        wait.until(EC.element_to_be_clickable(MATRIX_APP_BTN)).click()

        # --- PHASE 3: SWITCH WINDOWS ---
        wait.until(EC.number_of_windows_to_be(2))
        for window_handle in driver.window_handles:
            if window_handle != original_window:
                driver.switch_to.window(window_handle)
                break

        # --- PHASE 4: IDENTITY CONFLICT ---
        time.sleep(2)
        if "LoginIntermediateMLD" in driver.current_url:
            print("(!) Identity Conflict. Clicking Continue...")
            wait.until(EC.element_to_be_clickable((By.XPATH, IDENTITY_CONTINUE_XPATH))).click()

        # --- PHASE 5: CRMLS ALERTS ---
        dismiss_all_alerts(driver, FIRST_READ_LATER_BTN, context_name="CRMLS Alert")

        # --- PHASE 6: NAVIGATE TO LINKS ---
        print("Navigating to Links page...")
        links_btn = wait.until(EC.presence_of_element_located(NAV_LINKS_BTN))
        driver.execute_script("arguments[0].click();", links_btn)

        # --- PHASE 7: NWMLS ICON ---
        print("Locating NWMLS icon...")
        nwmls_btn = wait.until(EC.presence_of_element_located(NWMLS_ICON_BTN))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", nwmls_btn)
        time.sleep(1) 
        nwmls_btn.click()

        # --- PHASE 8: NWMLS ALERTS ---
        dismiss_all_alerts(driver, SECOND_READ_LATER_BTN, context_name="NWMLS Alert")

        # --- PHASE 9: NAVIGATE TO GENERAL SEARCH ---
        print("Navigating to General IntraMatrix Search...")
        search_link = wait.until(EC.presence_of_element_located(GENERAL_SEARCH_LINK))
        driver.execute_script("arguments[0].click();", search_link)

        # --- PHASE 10: INPUT PRICE ---
        print("Waiting for Search Form...")
        price_box = wait.until(EC.presence_of_element_located(PRICE_INPUT))
        print("Entering Price: 400-450")
        price_box.clear()
        price_box.send_keys("400-450")

        # --- PHASE 11: SELECT 'SOLD' STATUS ---
        print("Selecting 'Sold' status...")
        sold_cb = wait.until(EC.presence_of_element_located(SOLD_CHECKBOX))
        driver.execute_script("arguments[0].click();", sold_cb)
        sold_input = wait.until(EC.element_to_be_clickable(SOLD_DATE_INPUT))
        sold_input.clear()
        sold_input.send_keys("400-405")

        # --- PHASE 12: SELECT SUB TYPE ---
        print("Selecting 'Residential' from Sub Type...")
        sub_type_element = wait.until(EC.presence_of_element_located(SUB_TYPE_SELECT))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", sub_type_element)
        select_box = Select(sub_type_element)
        select_box.select_by_visible_text("Residential")

        # --- PHASE 13: CLICK SEARCH BUTTON ---
        print("Clicking Search Button...")
        search_btn = wait.until(EC.element_to_be_clickable(SEARCH_BTN))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_btn)
        time.sleep(1) 
        search_btn.click()

        # --- PHASE 14: RESULTS AND LOGGING ---
        print("Waiting for Results Table...")
        # Wait for at least one row to appear
        wait.until(EC.presence_of_element_located(RESULT_ROWS))
        
        # Get all rows
        rows = driver.find_elements(*RESULT_ROWS)
        print(f"Found {len(rows)} results. Processing first result...")

        if len(rows) > 0:
            target_row = rows[0] # We focus on the first row for now
            
            # -- Extract Data --
            # MLS Link (Contains the ID)
            mls_element = target_row.find_element(*MLS_LINK_SELECTOR)
            mls_id = mls_element.text
            
            # NOTE: The following indices [9], [13] etc are based on the HTML structure provided
            # Standard Matrix columns: Checkbox(0), Img(1), Map(2), MLS(3), Status(4), Type(5), Address(6)...
            # We get text from all cells to be safe
            cells = target_row.find_elements(By.TAG_NAME, "td")
            
            # Safely get text if cell exists
            address = cells[6].text if len(cells) > 6 else "N/A"
            city = cells[8].text if len(cells) > 8 else "N/A"
            price = cells[10].text if len(cells) > 10 else "N/A"
            
            print(f"Logging -> MLS: {mls_id} | Addr: {address} | City: {city} | Price: {price}")

            # -- Log to CSV --
            with open(CSV_FILENAME, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # Write Header if file is empty
                if file.tell() == 0:
                    writer.writerow(["MLS ID", "Address", "City", "Price"])
                writer.writerow([mls_id, address, city, price])
            
            print("Data saved to csv.")

            # -- Click MLS Link --
            print("Clicking MLS Link...")
            # Scroll into view first
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", mls_element)
            time.sleep(1)
            mls_element.click()
            
            print("SUCCESS: MLS Link clicked. Detail view opening...")

        time.sleep(10)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_scraper()