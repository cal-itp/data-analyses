"""
Project Sheet
Crosswalks
"""
# There are a few different ways the same agency is spelled in our project sheet.
grant_recipients_projects = {
   "Southern California  Regional Rail Authority": "Southern California Regional Rail Authority",
    
}

"""
Allocation Sheet
Crosswalks
"""
# Replacing date values
allocation_3rd_party_date = {"07/29/2020": "2020-07-29 00:00:00"}

allocation_led = {
    "2/1/2021\n\n10/31/2022": "2021-02-01 00:00:00",
    "June 30, 2019\nSeptember 30, 2019": "2019 06-30 00:00:00",
    "October 15, 2018\nSeptember 30, 2021": "2018-10-15 00:00:00",
    "Sep-23": "2023-09-01 00:00:00",
    "10/31/2022: 06/30/2024": "2024-06-30 00:00:00",
    "6/30/23\nPending": "2023-06-30 00:00:00",
    "3/24/2025\nPending": "2025-03-24 00:00:00",
}

allocation_allocation_date = {"8/16/2018\n12/5/2019": "2019-12-05 00:00:00"}

# Phase completion date 
allocation_completion_date = {
    "6/30/2021\n12/31/2021\n10/20/2022": "2021-06-30 00:00:00",
    "Complete\n8/30/2020": "2020-08-30 00:00:00",
    "Complete\n1/31/2020": "2020-01-31 00:00:00",
    "Complete\n6/1/2019": "2019-06-01 00:00:00",
    "Complete\n2/11/2018": "2018-02-11 00:00:00",
    "Complete\n6/30/2020": "2020-06-30 00:00:00",
    "Complete\n11/13/2019": "2019-11-13 00:00:00",
    "6/30/2020\nComplete": "2020-06-01 00:00:00",
    "May-24": "2024-05-01 00:00:00",
    "Dec-24": "2024-12-01 00:00:00",
    "Jun-22": "2022-06-01 00:00:00",
    "Feb-26": "2026-02-01 00:00:00",
    "Aug-25": "2025-08-01 00:00:00",
    "Nov-22": "2022-11-01 00:00:00",
    "Sep-23": "2023-09-01 00:00:00",
    "Jul-23": "2023-07-01 00:00:00",
    "Jan-30": "2030-01-01 00:00:00",
    "12/25/2021\nComplete": "2021-12-25 00:00:00",
    "Dec-23": "2023-12-01 00:00:00",
    "Nov-24": "2024-11-01 00:00:00",
    "Oct-22; June 30, 2024": "2024-06-30 00:00:00",
    "Feb-23": "2023-02-01 00:00:00",
    "Jul-22": "2022-07-01 00:00:00",
    "Jan-25": "2025-01-01 00:00:00",
    "Nov-23": "2023-11-01 00:00:00",
    "Dec-27": "2027-12-01 00:00:00",
    "Jan-24": "2024-01-01 00:00:00",
    "Apr-24": "2024-04-01 00:00:00",
    "Sep-24": "2024-09-01 00:00:00",
    "Jul-24": "2024-07-01 00:00:00",
    "Jun-23": "2023-61-01 00:00:00",
    "Apr-23": "2023-0401 00:00:00",
    "Jun-24": "2024-06-01 00:00:00",
    "Dec-25": "2025-25-01 00:00:00",
}

# PPNO are different in the allocation sheet. Correct them.
ppno_crosswalk_allocation = {
    "CP024": "CP043",
    "CP021": "CP043",
    "CP301": "CP031",
    "CP042": "CP031",
    "CP002": "CP000",
    "CP018": "CP019",
    "CP053": "CP052",
    "CP032": "CP034",
    "1155N": "1155A",
    "CP002": "CP000"

}

"""
Other
"""
# Adjust CT district to full names
full_ct_district = {
    7: "District 7: Los Angeles",
    4: "District 4: Bay Area / Oakland",
    "VAR": "Various",
    10: "District 10: Stockton",
    11: "District 11: San Diego",
    3: "District 3: Marysville / Sacramento",
     2: "District 2:Redding",
    12: "District 12: Orange County",
    8: "District 8: San Bernardino / Riverside",
    5: "District 5: San Luis Obispo / Santa Barbara",
    6: "District 6: Fresno / Bakersfield",
    1: "District 1: Eureka",
}

# Change county abbreviations to full name
full_county = {
    "LA": "Los Angeles",
    "VAR": "Various",
    "MON": "Mono",
    "ORA": "Orange",
    "SAC": "Sacramento",
    "SD": "San Diego",
    "SF": "San Francisco",
    "SJ": "San Joaquin",
    "SJ ": "San Joaquin",
    "FRE": "Fresno",
    "SBD": "San Bernandino",
    "SCL": "Santa Clara",
    "ALA": "Alameda",
    "SM": "San Mateo",
    "SB": "San Barbara",
    "SON, MRN": "Various",
}