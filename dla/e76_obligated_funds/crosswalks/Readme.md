## Methodology for creating Crosswalks 

Matching Agencies:
* First matched on Airtable Organizations and NTD Organiztaions on Name columns:
  * Organization Name
  * Acronym (using unmatched organizations from previous merge)
  * Doing Business As (using unmatched organizations from previous merges)
* Second round matching using NTD IDs (using unmatched organizations from previous merges)
* Add all matches and unmatches together to create an NTD/Airtable crosswalk/data origin csv
  * identify organizations that failed to match despite being a match and manually match

* Then matched with DLA Oragnizations using Name Columns:
  * Organization Name
  * Doing Business As (using unmatched organizations from previous merge)
  * Mobility Services Operated (using unmatched organizations from previous merge)
* Export as CSV to manually remove duplciates organizations (three instances)
