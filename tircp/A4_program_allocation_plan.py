'''
Program Allocation Plan
'''
def program_allocation_plan(): 
    ### LOAD IN SHEETS ### 
    df_project = project()
    df_allocation = allocation()
    #Only keeping certain columns
    df_project = (df_project[['Award_Year', 'Project_#','TIRCP_project_sheet','Local_Agency','Project_Title',
                              'PPNO', 'Unallocated_amt_project_sheet']]
                 )
    df_allocation = (df_allocation[['Award_Year','Award_Recipient', 'Implementing_Agency',
    'Components', 'PPNO','Phase',
    'Fiscal_Year_2020-2021', 'Fiscal_Year_2021-2022',
    'Fiscal_Year_2022-2023', 'Fiscal_Year_2023-2024',
    'Fiscal_Year_2024-2025', 'Fiscal_Year_2025-2026',
    'Fiscal_Year_2026-2027', 'Fiscal_Year_2027-2028',
    'Fiscal_Year_2028-2029', 'Fiscal_Year_2029-2030','CTC_Financial_Resolution',
    'Allocation_Date','Project_ID','SB1_Funding','GGRF_Funding','Allocation_Amt_Allocation_Sheet']]
                    ) 
    ### MERGE 2 SHEETS ###
    df_combined = df_allocation.merge(df_project, how = "left", on = ["PPNO", "Award_Year"])
    
    ### CLEAN UP ###
    #Fill in Project ID & CTC Fin Resolution with TBD so it'll show up
    df_combined[['Project_ID','CTC_Financial_Resolution']] = (df_combined[['Project_ID',
                                                            'CTC_Financial_Resolution']].fillna(value = 'TBD'))
    #Fill in missing dates with something random 
    missing_date = pd.to_datetime('2100-01-01')
    df_combined['Allocation_Date'] = df_combined['Allocation_Date'].fillna(missing_date)
    
    #Create Total_Amount Col
    df_combined['Total_Amount'] = df_combined['GGRF_Funding'] + df_combined['SB1_Funding']
    
    #Rename cols to the right names
    df_combined = (df_combined.rename(columns = {'TIRCP_project_sheet':'Award_Amount', 
                                                'Components': "Separable_Phases/Components",
                                                'CTC_Financial_Resolution': 'Allocation_Resolution',
                                                'SB1_Funding':'PTA-SB1_Amount', 
                                                'Unallocated_amt_project_sheet': 'Not_Allocated'})
                  ) 
    
    ### PIVOT ### 
    def pivot(df):
        df = df.groupby(['Award_Year','Project_#','Award_Amount','Not_Allocated','PPNO','Award_Recipient','Implementing_Agency',
        'Project_Title', 'Separable_Phases/Components','Phase','Project_ID','Allocation_Resolution','Allocation_Date']).agg({
        'Fiscal_Year_2020-2021': 'max',
        'Fiscal_Year_2021-2022': 'max', 'Fiscal_Year_2022-2023': 'max',
        'Fiscal_Year_2023-2024': 'max', 'Fiscal_Year_2024-2025': 'max',
        'Fiscal_Year_2025-2026': 'max', 'Fiscal_Year_2026-2027': 'max',
        'Fiscal_Year_2027-2028': 'max', 'Fiscal_Year_2028-2029': 'max',
        'Fiscal_Year_2029-2030': 'max', 'PTA-SB1_Amount': 'sum', 'GGRF_Funding':'sum',
         'Total_Amount':'sum'})
        return df 
    
    df_2015 = pivot(df_combined.loc[df_combined['Award_Year'] == 2015])
    df_2016 = pivot(df_combined.loc[df_combined['Award_Year'] == 2016])
    df_2018 = pivot(df_combined.loc[df_combined['Award_Year'] == 2018])
    df_2020 = pivot(df_combined.loc[df_combined['Award_Year'] == 2020])
    
    #GCS 
    with pd.ExcelWriter(f'{GCS_FILE_PATH}Program_Allocation_Plan.xlsx') as writer:
        df_2015.to_excel(writer, sheet_name="2015_Cycle_1", index=True)
        df_2016.to_excel(writer, sheet_name="2016_Cycle_2", index=True)
        df_2018.to_excel(writer, sheet_name="2018_Cycle_3", index=True)
        df_2020.to_excel(writer, sheet_name="2020_Cycle_4", index=True)
        
    return df_combined
    

