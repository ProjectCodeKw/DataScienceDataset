from selenium import webdriver
from selenium.webdriver.common.by import By
import time

driver = webdriver.Chrome()
url = "https://opencritic.com/outlet/553/ign-middle-east?sort=score-low"

driver.get(url)
time.sleep(3)

page_num = 1
html_files = {}

while True:
    # Save current page HTML
    html = driver.page_source
    filename = f"downloads/ign_page_{page_num}.html"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)
    
    html_files[page_num] = filename
    print(f"Saved page {page_num}: {filename}")
    
    # Try to find and click "Next" button
    try:
        next_button = driver.find_element(By.XPATH, "//a[contains(text(), 'Next')]")
        next_button.click()
        time.sleep(3)
        page_num += 1
    except:
        print("No more 'Next' button found")
        break

driver.quit()

print(f"\nDownloaded {page_num} pages")
print(f"Files: {list(html_files.values())}")