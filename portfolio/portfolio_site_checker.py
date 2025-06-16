# Portfolio site checker
# takes list of portfolio site URLs, run them through a loop to display the site's status code

import requests

portfolio_urls = [
        "https://analysis.calitp.org/legislative-district-digest/",
        "https://analysis.calitp.org/sb125-fund-split-analysis/",
        "https://analysis.calitp.org/ntd-monthly-ridership/",
        "https://analysis.calitp.org/ahsc/",
        "https://analysis.calitp.org/rt/",
        "https://analysis.calitp.org/ntd-annual-ridership-report/",
        "https://analysis.calitp.org/gtfs-digest/",
        "https://analysis.calitp.org/ha-starterkit-district/",
        "https://analysis.calitp.org/new-transit-metrics/",
        "https://analysis.calitp.org/district-digest/",
    ]

def portfolio_site_checker(portfolio_urls):
    
    for i in portfolio_urls:
        r = requests.get(i)
        if r.status_code == 200:
            print(f"Site: {i} is up")
        elif r.status_code == 404:
            print(f"Site: {i} is down")
        else:
            print(f"Site: {i}. status code:{r.status_code}")

if __name__ == "__main__":
    portfolio_site_checker(portfolio_urls)