"""
Project Sheet
Crosswalks
"""
# There are a few different ways the same agency is spelled.
# Clean them up.
grant_recipients_projects = {
    "San Joaquin Regional\nRail Commission / San Joaquin Joint Powers Authority": "San Joaquin Regional Rail Commission / San Joaquin Joint Powers Authority",
    "San Francisco Municipal  Transportation Agency": "San Francisco Municipal Transportation Agency",
    "San Francisco Municipal Transportation Agency (SFMTA)": "San Francisco Municipal Transportation Agency",
    "Capitol Corridor Joint Powers Authority (CCJPA)": "Capitol Corridor Joint Powers Authority",
    "Bay Area Rapid Transit (BART)": "Bay Area Rapid Transit District (BART)",
    "Los Angeles County Metropolitan Transportation Authority (LA Metro)": "Los Angeles County Metropolitan Transportation Authority",
    "Santa Clara Valley Transportation Authority (SCVTA)": "Santa Clara Valley Transportation Authority",
    "Solano Transportation Authority (STA)": "Solano Transportation Authority",
    "Southern California Regional Rail Authority (SCRRA - Metrolink)": "Southern California  Regional Rail Authority",
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
}
allocation_completion_date = {
    "6/30/2021\n12/31/2021\n10/20/2022": "2021-06-30 00:00:00",
    "Complete\n8/30/2020": "2020-08-30 00:00:00",
    "Complete\n1/31/2020": "2021-01-31 00:00:00",
}

# PPNO are different in the allocation sheet. Correct them.
ppno_crosswalk_allocation = {
    "CP018": "CP019",
    "CP024": "CP043",
    "CP021": "CP043",
    "CPO02": "CP025",
    "CP301": "CP031",
    "CP042": "CP031",
    "CP053": "CP052",
    "CP032": "CP034",
    "1155N": "1155A",
    "CP002": "CP000",
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