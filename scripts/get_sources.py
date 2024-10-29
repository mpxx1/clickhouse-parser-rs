from random import randint
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
import time
import re


def run() -> None:
    f = open('/Users/m/py/domclick_parse/links.txt', 'r')
    driver = uc.Chrome()

    counter = 0
    for line in f:

        counter += 1
        if counter < 1997:
            continue

        driver.get(line.strip())
        time.sleep(randint(1, 2))

        try:
            button = driver.find_element(By.CSS_SELECTOR, "[data-e2e-id='detail-spoiler']")
            driver.execute_script("arguments[0].scrollIntoView();", button)
            button.click()
        except:
            pass

        index = re.findall(r'\d+', line)

        try:

            target = open("/Users/m/py/domclick_parse/html_sources2/{}".format(index[0]), "w")
            target.write(driver.page_source)
            target.close()

        except FileNotFoundError:
            print(f"count: {counter}; Ошибка: директория 'html_sources2' или файл '{index[0]}' не найдены.")
        except IOError as e:
            print(f"count: {counter}; Ошибка ввода/вывода: {e}")
        except Exception as e:
            print(f"count: {counter}; Произошла непредвиденная ошибка: {e}")

        if counter % 100 == 0:
            print(counter)


if __name__ == "__main__":
    run()