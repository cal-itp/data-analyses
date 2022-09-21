# TIRCP 

**TIRCP**, The Transit and Intercity Rail Capital Program (TIRCP) was created by Senate Bill (SB) 862 (Chapter 36, Statutes of 2014) and modified by 9 (Chapter 710, Statutes of 2015), to provide grants from the Greenhouse Gas Reduction Fund (GGRF) to fund transformative capital improvements that will modernize California’s intercity, commuter, and urban rail systems, and bus and ferry transit systems to significantly reduce emissions of greenhouse gases, vehicle miles traveled, and congestion. [Source](https://dot.ca.gov/programs/rail-and-mass-transportation/transit-and-intercity-rail-capital-program)

## Sections

### [Presentations](./presentations/):
1. [CTC Presentation](./presentations/Interim_Expenditures_Solution.ipynb): Notebook for CTC presentation.
2. [Interim Expenditures](./presentations/Interim_Expenditures_Solution.ipynb): Notebook that takes expenditures data from InfoAdvantage and merges it with TIRCP data on Project ID. This was prepared for an internal PMP presentation.

### [Zero Emission Vehicles]:
1. [ZEV LCTOP TIRCP](./zev_lctop_tircp.ipynb): Notebook that estimates the number, type (LRV versus bus), and funding amount for purchasing ZEV through the LCTOP and TIRCP programs. The associated functions are under A7_zev.py.
3. [ZEV Charts](./zev_charts): The saved charts from the notebook. 


### [Other]:
1. [Tableau](./A2_tableau.py): This script prepares the Excel workbook that serves as the data source for the TIRCP Tableau dashboard. 
2. [Semiannual Report](./A3_semiannual_report.py): This script automates the Semiannual Report that summarizes how TIRCP projects have progressed.
3. [Program Allocation Plan](./A4_program_allocation_plan.py): This script automates the Program Allocation Plan that is submitted every few months to show how funds are scheduled to be allocated. 
4. [Crosswalks](./A5_crosswalks.py): There are manual portions of cleaning up this data and all the crosswalks are located here. 
5. [Full Script](./A6_full_script.py): A function that builds the first three reports and saves them to GCS. 
