# TIRCP 
**TIRCP**, The Transit and Intercity Rail Capital Program (TIRCP) was created by Senate Bill (SB) 862 (Chapter 36, Statutes of 2014) and modified by 9 (Chapter 710, Statutes of 2015), to provide grants from the Greenhouse Gas Reduction Fund (GGRF) to fund transformative capital improvements that will modernize California’s intercity, commuter, and urban rail systems, and bus and ferry transit systems to significantly reduce emissions of greenhouse gases, vehicle miles traveled, and congestion. [Source](https://dot.ca.gov/programs/rail-and-mass-transportation/transit-and-intercity-rail-capital-program)

## Sections
### Ad-Hoc Requests/Presentations 
1. [CTC Presentation](./requests/Interim_Expenditures_Solution.ipynb): Prep for CTC presentation.
2. [Interim Expenditures](./requests/Interim_Expenditures_Solution.ipynb): Takes expenditures data from InfoAdvantage and merges it with the Project sheet in the TIRCP Tracking Sheet on Project ID. This was prepared for an internal presentation.
3. [ZEV LCTOP TIRCP](./request_zev_lctop_tircp.ipynb): Prepared on the behalf of DRMT for a presentation to CARB about the # of ZEV purchased across TIRCP & LCTOP programs.
4. [Calsta](./request_calsta_tircp_outcomes.ipynb): Outcomes of TIRCP requested by Calsta's research team. 
5. [Sb1 GIS](./request_sb1_gis_template.ipynb): Populating SB1 GIS template using TIRCP that is used to create [this map](http://rebuildingca.ca.gov/map/). 

###  Scripts:
TIRCP scripts are based entirely on TIRCP Tracking Sheet 2.0 which is an Excel Workbook. The workbook is uploaded to GCS amd is saved as TIRCP_10-31-2022.xlsx with the updated date in the filename. 

<b>Before running the scripts</b>: open the Workbook and delete row 1:6 which are the header rows.

1. [Data Prep](./A1_data_prep.py): Cleans the sheets (allocation, project, GIS, and invoice) in the TIRCP workbook before creating other reports & the Excel file that feeds into Tableau. This file also contains functions used across this project. 
    * Replace FILE_NAME at the top with the latest file name. 
    * Run and follow the instructions in [this notebook](./script_manual.ipynb). The `Allocation` and `Project` sheets of the TIRCP workbook have areas that need to be manually looked over. The notebook does the following: 
        * Ensure PPNO numbers are unique to every project in the "Project Sheet." Sometimes projects are under different names in different cycles. There are 96 projects in total, as of writing.
        * Make sure PPNO numbers match across the "Allocation" and "Project" Sheets using sets and lists.  
        * Dates in the "Allocation" sheet are read in correctly and manually correct them if they are not. 
2. [Tableau](./A2_tableau.py): Run the function `complete_tableau()` to return an Excel workbook that serves as the data source for the TIRCP Tableau dashboard. 
3. [Semiannual Report](./A3_semiannual_report.py): Run `create_sar_report()` to create the Semiannual Report prepared by TIRCP's coordinator.
4. [Program Allocation Plan Report](./A4_program_allocation_plan.py): Automates the Program Allocation Plan Report (an Excel workbook) that is submitted every few months.
5. [Crosswalks](./A5_crosswalks.py): Some portions of the manual cleaning up are located here. 
6. [Other](./A6_other.py): Functions for the requests/presentations, such as basic charting, searching for keywords in the project descriptions, and extracting numbers from the project descriptions.  

