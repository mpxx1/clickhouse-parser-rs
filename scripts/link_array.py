from random import randint

import undetected_chromedriver as uc
import time
from bs4 import BeautifulSoup
import re



def run() -> None:

    driver = uc.Chrome()
    f = open('links.txt', 'a+')

    for i in range(0, 137):
        link = "https://spb.domclick.ru/search?deal_type=rent&category=living&offer_type=flat&from=topline2020&offset={}".format(i * 20)
        driver.get(link)
        time.sleep(randint(1, 3))

        soup = BeautifulSoup(driver.page_source, "html.parser")
        pattern = re.compile(r"https://spb\.domclick\.ru/card/rent__flat__\d+")

        links = {a['href'] for a in soup.find_all('a', href=True) if pattern.match(a['href'])}

        for link in links:
            f.write(link + '\n')


if __name__ == "__main__":
    run()