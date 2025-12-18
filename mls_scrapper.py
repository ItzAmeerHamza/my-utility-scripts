import time
import random
import csv
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

# --- CONFIGURATION ---
START_URL = "https://member.recenterhub.com/"
DASHBOARD_BUTTON_SELECTOR = ".app-card-mls" 

# --- SELECTORS ---
IDENTITY_CONTINUE_XPATH = "/html/body/form/div[4]/table/tbody/tr/td/table/tbody/tr/td[3]/a"
LINKS_NAV_SELECTOR = (By.CSS_SELECTOR, "a[href*='/Matrix/Links']")
LINKS_NAV_XPATH_BACKUP = "/html/body/form/div[3]/div[2]/div[2]/table/tbody/tr/td[2]/ul/li[8]/a"
NWMLS_LINK_SELECTOR = (By.CSS_SELECTOR, "a[title='NWMLS']")
ROSTER_TAB_SELECTOR = (By.ID, "navRoster") 
AGENT_ROSTER_ITEM_SELECTOR = (By.LINK_TEXT, "Agent Roster")
LAST_NAME_INPUT_XPATH = '/html/body/form/div[3]/div[6]/table/tbody/tr/td/div[2]/div[2]/div/div/div[1]/div[1]/div/table/tbody/tr[7]/td[3]/input'
SEARCH_BTN_ID = "m_ucSearchButtons_m_lbSearch"
PAGE_SIZE_ID = "m_ucDisplayPicker_m_ddlPageSize"
RESULT_ROW_SELECTOR = (By.CSS_SELECTOR, "div.singleLineDisplay")
READ_LATER_BTN_ID = "NewsDetailPostpone" 

# Selector for the "Next" button inside the paging span
NEXT_PAGE_XPATH = "//span[@class='pagingLinks']//a[contains(text(), 'Next')]"

def random_sleep(min_s=2, max_s=4):
    time.sleep(random.uniform(min_s, max_s))

def wait_for_page_load(driver, timeout=30):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
    except:
        pass

if __name__ == '__main__':
    options = uc.ChromeOptions()
    options.add_argument('--start-maximized') 
    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 30)

    try:
        # 1. LOGIN & DASHBOARD
        print(f"Navigating to: {START_URL}")
        driver.get(START_URL)
        input(">>> LOG IN, WAIT FOR DASHBOARD, THEN PRESS ENTER <<<")

        if "recenterhub" not in driver.current_url:
            driver.get("https://member.recenterhub.com/")
            wait_for_page_load(driver)

        # 2. OPEN MATRIX
        print("Clicking Dashboard button...")
        original_window = driver.current_window_handle
        dashboard_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, DASHBOARD_BUTTON_SELECTOR)))
        try:
            dashboard_btn.click()
        except:
            driver.execute_script("arguments[0].click();", dashboard_btn)
        
        wait.until(EC.number_of_windows_to_be(2))
        for window_handle in driver.window_handles:
            if window_handle != original_window:
                driver.switch_to.window(window_handle)
                break
        wait_for_page_load(driver)
        
        # 3. IDENTITY CONFLICT CHECK
        if "LoginIntermediateMLD" in driver.current_url:
            print("(!) Identity Conflict detected. Clicking Continue...")
            continue_btn = wait.until(EC.element_to_be_clickable((By.XPATH, IDENTITY_CONTINUE_XPATH)))
            continue_btn.click()
            wait.until(EC.url_contains("Default.aspx"))
            wait_for_page_load(driver)

        # 4. HANDLE "READ LATER" ALERTS
        print("Checking for News/Caution alerts...")
        while True:
            try:
                popup_wait = WebDriverWait(driver, 3)
                read_later_btn = popup_wait.until(EC.element_to_be_clickable((By.ID, READ_LATER_BTN_ID)))
                print("(!) Alert detected. Clicking 'Read Later'...")
                read_later_btn.click()
                time.sleep(2)
            except TimeoutException:
                print("No more alerts found. Proceeding...")
                break
        
        # 5. NAVIGATE TO LINKS & NWMLS
        print("Navigating to Links > NWMLS...")
        try:
            wait.until(EC.element_to_be_clickable(LINKS_NAV_SELECTOR)).click()
        except:
            driver.find_element(By.XPATH, LINKS_NAV_XPATH_BACKUP).click()
        
        nwmls_link = wait.until(EC.presence_of_element_located(NWMLS_LINK_SELECTOR))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", nwmls_link)
        time.sleep(1)
        nwmls_link.click()

        if len(driver.window_handles) > 2:
            driver.switch_to.window(driver.window_handles[-1])
        
        wait.until(EC.url_contains("nwmls")) 
        wait_for_page_load(driver)
        random_sleep(2, 4) 

        # 6. ROSTER NAV & SEARCH
        print("Navigating to Agent Roster & Searching...")
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#navRoster > a"))).click()
        wait.until(EC.element_to_be_clickable(AGENT_ROSTER_ITEM_SELECTOR)).click()

        wait_for_page_load(driver)
        last_name_input = wait.until(EC.visibility_of_element_located((By.XPATH, LAST_NAME_INPUT_XPATH)))
        last_name_input.clear()
        last_name_input.send_keys("z*")
        wait.until(EC.element_to_be_clickable((By.ID, SEARCH_BTN_ID))).click()

        # 7. SET PAGE SIZE
        print("Setting Page Size to 50...")
        wait_for_page_load(driver)
        page_size_dropdown = wait.until(EC.element_to_be_clickable((By.ID, PAGE_SIZE_ID)))
        select = Select(page_size_dropdown)
        select.select_by_value("50")
        
        # Wait for table reload after changing page size
        time.sleep(3) 

        # 8. SCRAPE DATA WITH PAGINATION LOOP
        print("Starting Data Extraction Loop...")
        csv_filename = "agent_roster_complete.csv"
        
        total_records_scraped = 0
        current_page = 1

        with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["Last Name", "First Name", "Contact Phone", "Email"])
            
            while True:
                print(f"--- Scraping Page {current_page} ---")
                
                # A. Wait for rows to be present
                try:
                    rows = wait.until(EC.presence_of_all_elements_located(RESULT_ROW_SELECTOR))
                except TimeoutException:
                    print("No rows found. Ending loop.")
                    break

                # B. Capture the first row element before we scrape or click next
                # We will use this element to detect when the page has refreshed
                old_first_row = rows[0]

                # C. Extract Data
                page_count = 0
                for row in rows:
                    try:
                        cols = row.find_elements(By.CSS_SELECTOR, "table.d24m2 td")
                        if len(cols) >= 9:
                            l_name = cols[2].text.strip()
                            f_name = cols[3].text.strip()
                            phone = cols[7].text.strip()
                            email = cols[8].text.strip()
                            writer.writerow([l_name, f_name, phone, email])
                            page_count += 1
                    except StaleElementReferenceException:
                        continue # If row updates mid-scrape, skip it
                
                total_records_scraped += page_count
                print(f"Scraped {page_count} records. Total: {total_records_scraped}")

                # D. Pagination Logic
                try:
                    next_btn = driver.find_element(By.XPATH, NEXT_PAGE_XPATH)
                    
                    # Check if disabled
                    if next_btn.get_attribute("disabled"):
                        print("Next button disabled. Done.")
                        break

                    # Scroll to and click Next
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    next_btn.click()
                    print("Clicking Next... Waiting for data reload...")

                    # --- CRITICAL WAIT BLOCK ---
                    # 1. Wait for the old row to become 'stale' (removed from DOM)
                    wait.until(EC.staleness_of(old_first_row))
                    
                    # 2. Wait for new rows to appear
                    wait.until(EC.presence_of_all_elements_located(RESULT_ROW_SELECTOR))
                    
                    # 3. Extra Safety Sleep: 
                    # Sometimes the table shell loads before the text inside fills in. 
                    # 2 seconds is a safe buffer for 50 records loading.
                    time.sleep(2)
                    # ---------------------------

                    current_page += 1

                except NoSuchElementException:
                    print("Next button not found. Done.")
                    break
                except Exception as e:
                    print(f"Pagination error: {e}")
                    break

        print("\n-------------------------------------------------")
        print(f"SUCCESS! Scraped {total_records_scraped} total records.")
        print(f"Data saved to {csv_filename}")
        print("-------------------------------------------------")
        
        input("Press Enter to close browser...")

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        driver.save_screenshot("error_scraping.png")

    finally:
        driver.quit()