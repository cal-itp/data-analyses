import A1_data_prep
import A2_tableau
import A3_semiannual_report 
import A4_program_allocation_plan
import A5_crosswalk 

import pandas as pd
from calitp import *

def complete_tircp():
    A2_tableau.complete_tableau()
    A3_semiannual_report.create_sar_report()
    A4_program_allocation_plan.create_program_allocation_plan()
    
if __name__ == '__main__': 
    complete_tircp()