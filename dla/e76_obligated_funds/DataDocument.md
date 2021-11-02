
## Data Documentation

Obligated and Waiting List for federal authorization of funds
#### Data Columns


Location: 

- Definition: Where the funds are in the allocation process
- Potential issues: none foreseen
- Dtype: object
- Example entries: Obligated, FTA transferred, District, FMIS, HQ, Obligated


Prefix: 

- Definition: Codes for grant types 
- Potential issues:
  - multiple program codes together to create a unique code
  - duplicates (misspelled or with a dash between)
- Dytpe: object
- Example entries: HSIP, HSIPL 


Project NO:

- Definition: Project Number, a combination of project number and Locodes (for some entries)
- Potential issues: 
  - project number includes mostly numbers, some with letters. 
  - Locodes are not used for all of a single agency. 
- Dtype: object
- Example entries: 5017(047), NBIL(516)


Agency: 

- Definition: Applicant name, agency of sorts 
- Potential issues: 
  - acronyms
  - spelling variations of the same agency/duplicates
  - capitalization 
- Dtype: object
- Example entries: Kern, Kern County. Kern County (District 9)


Prepared Date:

- Definition: Date funds are prepared, not to be confused with request date 
- Potential issues: 
  - formatting 
- Dtype: object
- Example entries: 2018-12-10, 10/12/20


Submit to HQ Data: 

- Definition: Date funds are prepared, not to be confused with request date 
- Potential issues: 
  -  formatting 
- Dtype: object
- Example entries: 2018-12-10, 10/12/20


HQ Review Date: 

- Definition: Date funds are prepared, not to be confused with request date 
- Potential issues: 
  - formatting 
- Dtype: object
- Example Entries: 2018-12-10, 10/12/20


Submit to FHWA Date: 

- Definition: Date funds are prepared, not to be confused with request date 
- Potential issues: 
  - formatting 
Dtype: object
Example entries: 2018-12-10, 10/12/20


To FMIS date: 

- Definition: Date funds are prepared, not to be confused with request date 
- Potential issues: 
  - formatting
- Dtype: object
- Example Entries: 2018-12-10, 10/12/20


Fed Requested: 

- Definition: Amount requested from the federal government 
- Potential issues:
  - formatting 
  - dollar signs and commas
- Dtype: float64
- Example entries:


AC Requested:

- Definition: Advance Construction funds requested
- Potential issues: 
  - formatting
  - dollar signs and commas
- Dtype: flaot64
- Example entries: $400,000 , 100000


Total Requested: 
 
- Definition: Total funds requested by Agency 
- Potential issues: 
  - some cells contain letters within the numbers creating a string 
- Dtype: object
- Example entries: '2748.3NA999'


Status Comment: 

- Definition: Comments on the status of funds, mostly helpful commentary in the waiting spreadsheet
- Potential issues: 
  - not standard reporting 
- Dtype: object
- Example entries: Authorized, Program Code


Locode: 

- Definition: A Local Agency Code - unique identifier for the MAs (master agreement) and other requests through Local Assistance 
- Potential issues: 
  - agencies have more than one locode
  - locodes have more than 4 numbers 
  - some with letters
  - Some with no locode associated
- Dtype: object
- Example entries: ‘NBIL’ (also a PREFIX code), 5910 


DIST: 

- Definition: District agency and project are located in 
- Potential issues: 
  - none foreseen
- Dtype: int64
- Example entries: 0, 1-12


Status:

- Definition: Status of the obligation
- Potential issues: 
  - varying dates located within the string 
  - approved do not have dates attached
- Dtype: object
- Example entries: Prepare on (date), E-76 approved on


Waiting Days: 

- Definition: Number of waiting days for obligation/authorization 
- Potential issues:
  - obligated funds have no waiting days 
- Dtype: float64
- Example entries: NA, 73


Dist Processing Days: 

- Definition: Number of days for a district to process obligation/request
- Potential issues:
  - negative days
- Dtype: float64
- Example Entries: 14, NA


HQ Processing Days:

- Definition: Number of days for Caltrans HQ to process obligation/request
- Potential issues: none foreseen
- Dtype: float64
- Example entries:30, 2


FHWA Processing Days: 

- Definition: Number of days for FHWA to process obligation/request
- Potential issues:
  - negative days
- Dtype: float64
- Example entries: 11, 38


FTIP No:

- Definition: FTIP (Federal Transportation Improvement Program) program codes
- Potential issues:
  - no direct connection to the PREFIX 
  - differences in local and rural in the same program
  - numbers attached
- Dtype: object
- Entries: HSIP_RURL, BRDGE-LUM

Project Location: 

- Definition: Where/what the funds are being applied
- Potential issues: 
  - variations in level of detail 
  - not specific enough to geoparse
  - includes multiple locations
  - empty cells
- Dtype: object
- Entries:  “Various locations within Yolo County”


Type of Work: 

- Definition: What the funds are being used for 
- Potential issues
  - empty cells
  - abbreviations
  - no spaces
- Dtype: object
- Entries: Permanent Restoration


SEQ: 

- Definition: Sequence in the project timeline 
- Potential issues: none foreseen
- Dtype: int64
- Example entries: 2, 10


Date Request Initiated: 

- Definition: Date the request for funds was initiated 
- Potential issues:
  - formatting
- Dtype: object
- Example entries: 2018-12-17, 12/2/19


Date Completed Request: 

- Definition: Date the request for funds was initiated 
- Potential issues:
  - formatting
- Dtype: object
- Example entries: 2018-12-20, 12/12/19


MPO: 

- Definition: Metropolitan Planning Organization where agency/project is located
- Potential issues: 
  - acronym spelling (NONMPO NON-MPO) 
- Dtype: object
- Example entries: SCAG, MTC


Today: 

- Definition: Date of download
- Potential issues: none, other than the field is blank
- Dtype: object
- Example entries: 11/1/21


Waiting: (only on waiting sheet) 

- Definition: Issues with the funding request
- Potential issues: 
  - Red text doesn't translate, loses meaning 
- Dtype: object
- Example entries: Warning, negatie or zero funding



