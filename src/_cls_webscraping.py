"""
create_at: 2024-02-19 17:28:59
edited_at: 2025-01-21 17:59:00

@author: ronald.barberi
"""

#%% Imported libraries

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#%% Create Class

class WebScrapinKreton:
    def funInitBrowser(navegatorType: str,
                       initGPU: bool,
                       varDriverPath: str, 
                       varProfilePath=None,
                       varDownloadPath=None
        ):

        if navegatorType == 'chrome':
            options = webdriver.ChromeOptions()
            options.add_argument('--disable-popup-blocking')
            options.add_argument('--disable-notifications')
            options.add_argument('--start-maximized')
            if not initGPU:
                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument('--ignore-certificate-errors')
                options.add_argument('--log-level=3')
                options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
                options.add_experimental_option('excludeSwitches', ['enable-logging'])
                options.add_argument('window-size=1920x1080')
            
            if varProfilePath:
                options.add_argument(f'user-data-dir={varProfilePath}')
            
            if varDownloadPath:
                prefs = {'download.default_directory': varDownloadPath}
                options.add_experimental_option('prefs', prefs)
            
            service = Service(executable_path=varDriverPath)
            driver = webdriver.Chrome(service=service, options=options)


        elif navegatorType == 'edge':
            options = webdriver.EdgeOptions()
            options.add_argument('--disable-popup-blocking')
            options.add_argument('--disable-notifications')
            options.add_argument('--start-maximized')
            if not initGPU:
                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument('--ignore-certificate-errors')
                options.add_argument('--log-level=3')
                options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
                options.add_experimental_option('excludeSwitches', ['enable-logging'])
                options.add_argument('window-size=1920x1080')
            
            if varProfilePath:
                options.add_argument(f'user-data-dir={varProfilePath}')
            
            if varDownloadPath:
                prefs = {'download.default_directory': varDownloadPath}
                options.add_experimental_option('prefs', prefs)
            service = Service(executable_path=varDriverPath)
            driver = webdriver.Edge(service=service, options=options)
        else:
            print(f'Option on valid: {navegatorType}')
            return None

        driver.maximize_window()
        return driver


    def funJoinLink(driver, link):
        driver.get(link)


    def funClickNav(driver, typeElement: str, elementWeb: str):
        if typeElement == 'xpath':
            method = By.XPATH
        elif typeElement == 'css':
            method = By.CSS_SELECTOR
        elif typeElement == 'class':
            method = By.CLASS_NAME
        elif typeElement == 'link':
            method = By.LINK_TEXT
        elif typeElement == 'partial':
            method = By.PARTIAL_LINK_TEXT
        elif typeElement == 'tag':
            method = By.TAG_NAME
        elif typeElement == 'name':
            method = By.NAME
        else:
            print(f'the {typeElement} is incorrect')

        if typeElement == 'id':
            button = driver.find_element(By.ID, elementWeb)
            driver.execute_script('arguments[0].click();', button)

        button = driver.find_element(method, elementWeb)
        button.click()


    def funWaitToClick(driver, typeElement: str, timeWait: int, elementWeb: str):
        if typeElement == 'xpath':
            method = By.XPATH
        elif typeElement == 'css':
            method = By.CSS_SELECTOR
        elif typeElement == 'class':
            method = By.CLASS_NAME
        elif typeElement == 'link':
            method = By.LINK_TEXT
        elif typeElement == 'partial':
            method = By.PARTIAL_LINK_TEXT
        elif typeElement == 'tag':
            method = By.TAG_NAME
        elif typeElement == 'id':
            method = By.ID
        elif typeElement == 'name':
            method = By.NAME
        else:
            print(f'the {typeElement} is incorrect')
        
        button = WebDriverWait(driver, timeWait).until(
                EC.element_to_be_clickable((method, elementWeb))
            )
        button.click()


    def funInsertKeysNav(driver, typeElement: str, elementWeb: str, textInsert: str):
        if typeElement == 'xpath':
            method = By.XPATH
        elif typeElement == 'css':
            method = By.CSS_SELECTOR
        elif typeElement == 'class':
            method = By.CLASS_NAME
        elif typeElement == 'link':
            method = By.LINK_TEXT
        elif typeElement == 'partial':
            method = By.PARTIAL_LINK_TEXT
        elif typeElement == 'tag':
            method = By.TAG_NAME
        elif typeElement == 'id':
            method = By.ID
        elif typeElement == 'name':
            method = By.NAME
        else:
            print(f'the {typeElement} is incorrect')

        keys = driver.find_element(method, elementWeb)
        keys.send_keys(textInsert)


    def funWaitToElement(driver, typeElement: str, timeWait: int, elementWeb: str):
        if typeElement == 'xpath':
            method = By.XPATH
        elif typeElement == 'css':
            method = By.CSS_SELECTOR
        elif typeElement == 'class':
            method = By.CLASS_NAME
        elif typeElement == 'link':
            method = By.LINK_TEXT
        elif typeElement == 'partial':
            method = By.PARTIAL_LINK_TEXT
        elif typeElement == 'tag':
            method = By.TAG_NAME
        elif typeElement == 'id':
            method = By.ID
        elif typeElement == 'name':
            method = By.NAME
        else:
            print(f'the {typeElement} is incorrect')

        WebDriverWait(driver, timeWait).until(
            EC.presence_of_element_located((method, elementWeb)))


    def funSelectOption(driver, name_id, text):
        source = driver.find_element(By.NAME , name_id)
        source_select = Select(source)
        source_select.select_by_visible_text(f'{text}')


    def funClearCamp(driver, typeElement: str, elementWeb: str):
        if typeElement == 'xpath':
            method = By.XPATH
        elif typeElement == 'css':
            method = By.CSS_SELECTOR
        elif typeElement == 'class':
            method = By.CLASS_NAME
        elif typeElement == 'link':
            method = By.LINK_TEXT
        elif typeElement == 'partial':
            method = By.PARTIAL_LINK_TEXT
        elif typeElement == 'tag':
            method = By.TAG_NAME
        elif typeElement == 'id':
            method = By.ID
        elif typeElement == 'name':
            method = By.NAME
        else:
            print(f'the {typeElement} is incorrect')
            
        input_element = driver.find_element(method, elementWeb)
        input_element.clear()
