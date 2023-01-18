from serpapi import GoogleSearch
from requests_html import HTMLSession
import csv
import threading
import time
import yaml


class Google:
    def __init__(self, api, query, location):
        self.query = query
        self.location = location

        self.params = {
            "q": self.query,
            "location": self.location,
            "tbm": "lcl",
            "num": 100,
            "api_key": "",
            "start": 0
        }

        self.pages = 3
        self.result = None
    
    def search(self):
        results = []
        for i in range(self.pages):
            search = GoogleSearch(self.params)
            result = search.get_dict()
            results.extend(self.parse_result(result))
            self.params['start'] += 20
        return results

    def parse_result(self, result):
        companies = []
        for company in result['local_results']:
            try:
                if 'title' in company:
                    name = company['title']
                else:
                    continue
                if 'type' in company:
                    type = company['type']
                else:
                    type = None
                if 'address' in company:
                    location = company['address']
                else:
                    location = self.location
                if 'phone' in company:   
                    phone = company['phone']
                else:
                    phone = None
                if 'links' in company and 'website' in company['links']:
                    website = company['links']['website']
                else:
                    website = None
                companies.append(Company(name, type, location, phone, website))
            except Exception as e:
                print(e)
        return companies


class Company:
    def __init__(self, name, type, location, phone, website):
        self.company_name = name
        self.type = type
        self.location = location
        self.phone = phone
        self.website = website
        self.email = email_scrape(self.website)

    def __str__(self):
        return f"{self.company_name}, {self.type}, {self.location}, {self.phone}, {self.website}, {self.email}"


def email_scrape(site):
    try:
        session = HTMLSession()
        r = session.get(site)
        email = r.html.find('a[href^="mailto:"]', first=True)
        if email:
            return email.attrs['href'][7:]
        else:
            return None
    except:
        return None


def load_data():
    with open('config/searches.txt', 'r') as f:
        searches = f.readlines()
    searches = [search.strip() for search in searches]

    with open('config/locations.txt', 'r') as f:
        locations = f.readlines()
    locations = [location.strip() for location in locations]

    return searches, locations


def save_data(company):
    if check_duplicate(company.company_name):
        with open('companies.csv', 'a') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(company.__dict__.values())


def check_duplicate(company):
    with open('companies.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        if company not in [c[0] for c in reader]:
            return True


def main():
    data_fields = ['Company', 'Type', 'Location', 'Phone', 'Website', 'Email']
    with open('companies.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data_fields)

    config = yaml.load(open('config/config.yaml'), Loader=yaml.FullLoader)
    
    pool = []
    searches, locations = load_data()
    for search in searches:
        for location in locations:
            t = threading.Thread(target=scrape, args=(config['api_key'], search, location))
            t.start()
            pool.append(t)
            time.sleep(1)
        for t in pool:
            t.join()
    

def scrape(api_key, search, location):
    google = Google(api_key, search + " near me", location)
    companies = google.search()
    for company in companies:
        save_data(company)


if __name__ == '__main__':
   main()

    