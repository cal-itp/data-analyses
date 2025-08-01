# Portfolio site checker
# takes list of portfolio site URLs, run them through a loop to display the site's status code

import requests

dds_url = "https://analysis.dds.dot.ca.gov/"
calitp_url = "https://analysis.calitp.org/"

portfolio_names =[
    "legislative-district-digest/",
    "sb125-fund-split-analysis/",
    "ntd-monthly-ridership/",
    "ahsc/",
    "rt/",
    "ntd-annual-ridership-report/",
    "gtfs-digest/",
    "ha-starterkit-district/",
    "new-transit-metrics/",
    "district-digest/", 
]

def portfolio_site_checker(base_url,site_names):
    
    for site in sorted(site_names):
        r = requests.get(base_url+site)
        if r.status_code == 200:
            print(f"Site: {base_url+site} is up")
        elif r.status_code == 404:
            print(f"Site: {base_url+site} is down")
        else:
            print(f"Site: {base_url+site}. status code:{r.status_code}")

if __name__ == "__main__":
    print("\nChecking dds.dot.ca.gov portfolio sites")
    portfolio_site_checker(base_url= dds_url, site_names=portfolio_names)
    
    print("\nChecking calitp.org portfolio sites")
    portfolio_site_checker(base_url= calitp_url, site_names=portfolio_names)
    
