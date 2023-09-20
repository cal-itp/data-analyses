'''
Utils for Report Data Prep and Reports:

Functions combine the funded and application data to get one row for each project. 
From there was can use the functions for the report
'''

import pandas as pd
import geopandas as gpd
from calitp_data_analysis.sql import to_snakecase
from dla_utils import _dla_utils
from shared_utils import geography_utils
from shared_utils import calitp_color_palette as cp

from IPython.display import HTML
from siuba import *

import altair as alt

import _data_cleaning



'''
Read Data
'''

GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/dla/atp/'


## function reads in funding data (projects identified to get funding with funding amounts)
def read_SUR_funding_data():
    """
    Function to read in ATP funding data. Function will need to change for future data.
    Notes:
    * `atp_id` columns appear the same but the sur_details has an extra zero in the middle of the string so it would not match
    * `a3_project_type` also is entered differently however, details has more details than the funding sheet. Has information on size of project. can add to new column
    * `a1_imp_agcy_name_x` has manual errors so selecting `a1_imp_agcy_name_y`
    """
    # identify information columns that we need to drop
    columns_to_drop = ['a1_imp_agcy_contact','a1_imp_agcy_email','a1_imp_agcy_phone',
                      'a1_proj_partner_contact', 'a1_proj_partner_email', 'a1_proj_partner_phone']
    #read in SUR details and SUR funding data
    sur_details = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Master_AllData_Cycle5_Field_Mapping_COPY.xls',
              sheet_name='Statewide SUR Details'))
    sur_details = sur_details.drop(columns = columns_to_drop)
    
    sur_funding = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Master_AllData_Cycle5_Field_Mapping_COPY.xls',
              sheet_name='Statewide SUR Funding'))
    
    #drop the last few columns of SUR Details that have no funding data entered, but have columns
    sur_details.drop(sur_details.iloc[:,199:], inplace=True, axis=1)
    
    #remove rows with all null values
    cols_to_check = sur_funding.columns
    sur_funding['is_na'] = sur_funding[cols_to_check].isnull().apply(lambda x: all(x), axis=1) 
    sur_funding = sur_funding>>filter(_.is_na==False)
    sur_funding = sur_funding.drop(columns={'is_na'})

    #delete rows identified that are not part of the data (informational cells) or a sum total for all entries
    delete_row = sur_funding[sur_funding["project_cycle"]== 'Added Field not from App'].index
    sur_funding = sur_funding.drop(delete_row)
    
    delete_row = sur_funding[sur_funding["total_project_cost"]== '370,984,000.00'].index
    sur_funding = sur_funding.drop(delete_row)
    
    #merge sur_funding and sur_details
    merge_on = ['project_app_id', 'project_cycle', 'a2_ct_dist', 'a1_locode']
    
#     sur_details['a1_locode'] = sur_details['a1_locode'].replace('None', 0)
#     sur_funding['a1_locode'] = sur_funding['a1_locode'].replace('None', 0)
    
    sur_details['a1_locode'] = pd.to_numeric(sur_details.a1_locode, errors='coerce')  
    sur_funding['a1_locode'] = pd.to_numeric(sur_funding.a1_locode, errors='coerce')
#     fix_merge_cols = ['project_cycle', 'a2_ct_dist', 'a1_locode']
    
#     sur_funding[fix_merge_cols] = sur_funding[fix_merge_cols].astype('Int64')
#     sur_details[fix_merge_cols] = sur_details[fix_merge_cols].astype('Int64')
    
    
    ### CHANGING TO INNER CHECK LATER
    df = (pd.merge(sur_details, sur_funding, how="inner", on = merge_on, indicator=True))
    
    #keep entries that merge. Right_only rows are misentered and more informational columns  
    df = df>>filter(_._merge=='both')
    
    # filling the null values for some of the duplicate columns
    # manually checking that values are the same as of now- will add function to check when we get the data links
    df['awarded_x'] = df['awarded_x'].fillna(df['awarded_y'])
    df['ppno_y'] = df['ppno_y'].fillna(df['ppno_x'])
    
    #renaming and dropping duplicate columns 
    ## a1_imp_agcy_name_x has manual errors so selecting a1_imp_agcy_name_y
    df = df.rename(columns={'awarded_x':'awarded',
                                'ppno_y':'ppno',
                                'a1_imp_agcy_name_y':'a1_imp_agcy_name',
                                'a2_info_proj_name_y':'a2_info_proj_name'
                               })
    df = df.drop(columns={'awarded_y', 'a1_imp_agcy_name_x', 'a2_info_proj_name_x','ppno_x', '_merge'})
    df["data_origin"]="Funded"
    
    return df


## put together funding data and application data
def join_funding_and_app_data(df_funding,
                              df_app,
                              awarded_col: list = [],
                             sort_values_cols: list = [],
                             subset_cols: list = []
                             ):
    '''
    columns in the funded and application data that we want to use
    awarded_col= ['awarded'],
    sort_values_cols = ['project_app_id','a2_proj_scope_summary', 'project_cycle', 'awarded'],
    subset_cols = ['project_app_id','a2_proj_scope_summary','project_cycle']
    '''
    
    # concat the funding and app dataframes
    df = (pd.concat([df_app, df_funding]))
    
    # take the awarded column and convert to a category so we can order by this column
    df[awarded_col] = df[awarded_col].astype('category') 
  #  df[awarded_col] = df[awarded_col].cat.set_categories(['Y', 'N'], ordered=True) 
    
    # sort values based on columns we defined (usually key like unique id, cycle)
    df.sort_values(sort_values_cols, inplace=True, ascending=True) 
    
    # drop duplicates so we only get the funded data instead of the application data for a project that is selected
    df_final = df.sort_values(awarded_col).drop_duplicates(subset=subset_cols, keep='last')
    
    return df_final
    

## read in the joined data so we only have to use one function
def read_in_joined_data():
    app_data = pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/atp/cleaned_cycle5&6.xlsx',  index_col=[0])
    funded_data = read_SUR_funding_data()
    
    df = join_funding_and_app_data(funded_data,
                                   app_data, 
                                   awarded_col= ['awarded'],
                                   sort_values_cols = ['project_app_id','a2_proj_scope_summary', 'project_cycle', 'awarded'],
                                   subset_cols = ['project_app_id','a2_proj_scope_summary','project_cycle'])
    
    ## read in county names to change acronym names
    ## using place names
    county_place_names = (to_snakecase(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/2020-place-names-locode.xlsx', sheet_name=1)))
    
    county_place_names = county_place_names>>select(_.county_name, _.co__name_abbr_)
    county_place_names['co__name_abbr_'] = county_place_names['co__name_abbr_'].str.upper()
    ## create dictionary to map full county names
    county_map = dict(county_place_names[['co__name_abbr_', 'county_name']].values)
    
    #map and fillna values with full county names from original column
    df['a2_county_2'] = df.a2_county.map(county_map)
    df['a2_county_2'] = df['a2_county_2'].fillna(df['a2_county'])
    
    # drop original column and rename county column
    columns_to_drop = ['a2_county']
    df = df.drop(columns = columns_to_drop)
    df = df.rename(columns= {'a2_county_2':'a2_county'})
    
    ## Reorder cols to get app id in the front of the data frame
    ## https://stackoverflow.com/questions/41968732/set-order-of-columns-in-pandas-dataframe
    cols_to_order = [ 'project_app_id', 'project_cycle', 'a1_locode',
                 '#', 'atp_id', 'awarded', 'ppno', 'ppno_1',
                 'data_origin', 'geometry','project_status',
                 'solicitation_abv', 'solicitation', 'soliciting_agency', 'project_size',
                 'a1_imp_agcy_city', 'a1_imp_agcy_name', 
                 'a1_proj_partner_agcy', 'a1_proj_partner_exists',
                 'assembly_district', 'congressional_district', 'senate_district', 'a2_county', 'a2_ct_dist', 
                 'a2_info_proj_descr', 'a2_info_proj_loc', 'a2_info_proj_name', 'a2_mop_uza_population',
                 'a2_mpo', 'a2_rtpa',  'a2_proj_scope_summary']
        
    new_columns = cols_to_order + (df.columns.drop(cols_to_order).tolist())
    df = df[new_columns]
    
    df['a1_imp_agcy_city'] = df['a1_imp_agcy_city'].str.title()
    
    df['a2_county'] = df['a2_county'].str.replace('MPA ','Mariposa')

    
    return df

'''
Map geometry cleaning
'''

def check_point_in_state(df,
                        state_col,
                        state_col_value):
    df[state_col] = df[state_col].fillna('None')
    def validate_point(df):
        if (df[state_col] == 'None'):
            return 'Point Not In State'
        
        elif (df[state_col] == state_col_value):
            return 'Point In State'
        
        return df
    
    df['point_check'] = df.apply(validate_point, axis = 1)
    
    return df

## determine if the point is within CA State boundary
def join_state_with_points(gdf):
    
    if 'index_right' in gdf.columns:
        gdf = gdf.drop(columns=['index_right','State','point_check'])

    #read in state polygon
    ca_bounds = gpd.read_parquet('gs://calitp-analytics-data/data-analyses/high_quality_transit_areas/ca_boundary.parquet')
    
    #join together with geodf
    joined = (gdf.sjoin(ca_bounds, how='left'))
    
    joined_check = check_point_in_state(joined,
                        'State',
                        'CA')
    
    return joined_check



##use city place names to get correct point geometries for implementing agencies

def get_latlong_from_placenames(df, city_col):
    #read in place names data
    places = "https://data.ca.gov/dataset/e212e397-1277-4df3-8c22-40721b095f33/resource/436fc714-831c-4070-b44b-b06dcde6bf18/download/ca-places-boundaries.zip"
    places_ca = gpd.read_file(places)
    
    #get centroid and subset columns
    places_ca['geometry2'] = places_ca['geometry'].centroid
    places_ca= places_ca>>select(_.NAME, _.NAMELSAD, _.INTPTLAT, _.INTPTLON)
    
    df = df.drop(columns=['index_right','State','point_check', 'geometry','a2_proj_lat','a2_proj_long']) 
    
    df_fixed = (pd.merge(df, places_ca,
           how = 'left',
           left_on= [city_col],
           right_on='NAME'))
    
    
    df_fixed = (geography_utils.create_point_geometry(df_fixed,
                                                    longitude_col = 'INTPTLON',
                                                    latitude_col = 'INTPTLAT'))
    df_fixed = df_fixed.rename(columns={'INTPTLAT':'a2_proj_lat',
                                       'INTPTLON':'a2_proj_long'})
    
    df_fixed = df_fixed.drop(columns=['NAME','NAMELSAD']) 

    
    return df_fixed

## fix all geo data points in one function. returns geodataframe with points in CA Bounds
def fix_geom_issues(df, subset_col_list):
    
    ## susbet dataframe based on a list of columns
    df_map = df[df.columns.intersection(subset_col_list)]
    ## create geometry points from lat long col
    df_map = (geography_utils.create_point_geometry(df_map, longitude_col = 'a2_proj_long', latitude_col = 'a2_proj_lat'))
    
    ##use join_state_with_points to determine which points are in state or not 
    ##will add the fixed geometries back to this in after
    joined = ((join_state_with_points(df_map))>>filter(_.point_check=='Point In State'))
    ## subset the rows that have incorrect lat longs 
    need_help = ((join_state_with_points(df_map))>>filter(_.point_check=='Point Not In State'))
    
    ## get those that have a negative long
    need_help_pt1 = need_help>>filter(_.a2_proj_long<0)
    ## get those with a positive long (to change into a negative long)
    need_help_pt2 = need_help>>filter(_.a2_proj_long>0)
    
    ## get those with abnormal lats 
    need_help_pt3 = need_help_pt2[(need_help_pt2['a2_proj_lat'] > 300) | (need_help_pt2['a2_proj_long'] < 50)]
    ## get those with normal lats and positive longs
    need_help_pt2 = need_help_pt2[(need_help_pt2['a2_proj_lat'] < 300) & (need_help_pt2['a2_proj_long'] > 50)]
    
    ## concat need_help1 and need-help3 to have a some that we can use the implementing agency's city. 
    need_help_pt1 = (pd.concat([need_help_pt1, need_help_pt3])).drop_duplicates()
    
    ## multiply longs by -1 to get correct long 
    need_help_pt2['a2_proj_long'] = need_help_pt2['a2_proj_long'] * (-1)
    ## redo geom points for fixed longs
    need_help_pt2 = (geography_utils.create_point_geometry(need_help_pt2, longitude_col = 'a2_proj_long', latitude_col = 'a2_proj_lat'))
    
    ## recheck if points are fixed to be in state
    need_help_pt2 = (join_state_with_points(need_help_pt2))
    
    ## add in those with points not in state to the other entries
    need_help_pt1 = pd.concat([need_help_pt1, (need_help_pt2>>filter(_.point_check=='Point Not In State'))])
    
    ## get those just with in state points as finished.
    need_help_pt2_done = need_help_pt2>>filter(_.point_check=='Point In State')
    
    ## use place names
    need_help_pt1 = get_latlong_from_placenames(need_help_pt1, 'a1_imp_agcy_city')
    ## make sure points are in state again
    need_help_pt1_done = (join_state_with_points(need_help_pt1))
    
    ##concat together
    fixed = pd.concat([need_help_pt1_done, need_help_pt2_done])
    
    ## assert there are no "Point Not In State" Values in columns
    assert (len(fixed>>filter(_.point_check=='Point Not In State')))==0
    
    final_df = pd.concat([joined, fixed])

    return final_df




'''
create dfs for mapping geographies for districts (ct, congressional, assembly, etc)
'''
## join a geodf and a main data df
def nunique_by_geography(df,
                         geodf,
                         groupby_cols: list = [],
                         agg_col: list = [],
                         geodf_merge_on_col: list = [],
                         sum_merge_on_col: list = []):
    
    sum_df = (df>>group_by(*groupby_cols)
              >>summarize(n_unique = _[agg_col].nunique()))
    
    sum_df = (sum_df>> group_by(_[sum_merge_on_col]) >>spread(_.data_origin, _.n_unique)).apply(lambda x: x) 
    
    joined_df = geodf.merge(sum_df, 
                            how='left',
                            left_on=[geodf_merge_on_col],
                            right_on=[sum_merge_on_col])
    
    joined_df = joined_df.fillna(0)
    
    joined_df['Total'] = joined_df['Application'] + joined_df['Funded']
    joined_df['Success Rate'] = (joined_df['Funded'] / joined_df['Application'])
    
    return joined_df
              
    
## explode columns that haven multiple districts listed in them
def explode_and_join_geo(df,
                         geo_df, 
                         explode_cols: str,
                         groupby_cols: list = [],
                         count_col: list = [],
                         geo_df_merge_col: list = []):
    
    list_of_cols = groupby_cols + [explode_cols]
    
    df_subset = df>>select(*list_of_cols) 
    
    df_subset = df_subset.replace('Needs Manual Assistance', 0)
    
    df_subset[explode_cols] = df_subset[explode_cols].astype(str)

    df_subset[['first_dist', 'second_dist', 'third_dist']] = df_subset[explode_cols].str.split(', ', expand=True)
    
    select_cols = groupby_cols + ['dist'] + ['data_origin']
    agg_df = (df_subset 
              >> gather('measure', 'dist',_.first_dist, _.second_dist, _.third_dist)
              >> select(*select_cols)
              >> filter(_.dist.notnull())
             )
    
    final_df = nunique_by_geography(agg_df,
                         geo_df,
                         groupby_cols = ['dist', 'data_origin'],
                         agg_col = count_col,
                         geodf_merge_on_col = geo_df_merge_col,
                         sum_merge_on_col = 'dist')

    return final_df

    
'''
Report Functions
'''

def reorder_namecol(df,
                    og_name_col,
                    new_name_col, 
                    split_on,
                   order_on):
    
    df_copy = df>>filter(_[og_name_col].str.contains(split_on))>>select(_[og_name_col])
    df_copy[['name_pt1', 'name_pt2']] = df_copy[og_name_col].str.split(split_on, 1, expand=True)
    
    def order_new_name(df):
        if order_on == "pt1_pt2":
            return (df['name_pt1'] + ' ' + df['name_pt2'])
        elif order_on == "pt2_pt1":
            return (df['name_pt2'] + ' ' + df['name_pt1'])
        else:
            return (df['name_pt1'] + ' ' + df['name_pt2'])
        
    df_copy[new_name_col] = df_copy.apply(order_new_name, axis = 1)
    
    new_name_mapping = (dict(df_copy[[og_name_col, new_name_col]].values))
    
    df[new_name_col] = df[og_name_col].map(new_name_mapping)

    df[new_name_col] = df[new_name_col].fillna(df[og_name_col])
    

    
    return df

def prep_mapping_data(df):
    mapsubset_cols= ['awarded','project_app_id', 'project_cycle', 'data_origin', 'geometry',
               'a1_imp_agcy_city','imp_agency_name_new','a1_proj_partner_agcy', 
               'assembly_district','congressional_district','senate_district',
              'a2_county', 'a2_ct_dist','a2_info_proj_name','a3_proj_type', 'total_atp_$', 'a2_proj_lat','a2_proj_long']
    df_map = fix_geom_issues(df, mapsubset_cols)
    df_map = df_map.drop(columns='index_right')
    
    return df_map

def clean_df_shapes(shapes):
    shapes_clean = to_snakecase(gpd.read_file(f"{shapes}").to_crs(epsg=4326))
    shapes_clean = shapes_clean >>select(_.district, _.population, _.geometry)
    
    return shapes_clean

## function to read in congressional, senate and assembly shape data

def read_in_district_shapes(gdf):
    #clean df shapes function 
    assemblyshapes = clean_df_shapes("https://gis.data.ca.gov/datasets/f173bfa16514414ab6130c248fdd9d28_0.geojson")
    congressionalshapes = clean_df_shapes("https://gis.data.ca.gov/datasets/f173bfa16514414ab6130c248fdd9d28_2.geojson")
    senateshapes = clean_df_shapes("https://gis.data.ca.gov/datasets/f173bfa16514414ab6130c248fdd9d28_1.geojson")
    
    #use function explode_and_join to combine shapes and data
    assembly_districts = explode_and_join_geo(gdf,
                                                      assemblyshapes,
                                                      explode_cols = 'assembly_district',
                                                      groupby_cols=['project_app_id','project_cycle','data_origin','a1_imp_agcy_city'],
                                                      count_col= 'project_app_id',
                                                      geo_df_merge_col = 'district')
    congressional_districts = explode_and_join_geo(gdf,
                         congressionalshapes,
                         explode_cols = 'congressional_district',
                         groupby_cols=['project_app_id','project_cycle','data_origin','a1_imp_agcy_city'],
                         count_col= 'project_app_id',
                         geo_df_merge_col = 'district')
    senate_districts = explode_and_join_geo(gdf,
                                                      senateshapes,
                                                      explode_cols = 'senate_district',
                                                      groupby_cols=['project_app_id','project_cycle','data_origin','a1_imp_agcy_city'],
                                                      count_col= 'project_app_id',
                                                      geo_df_merge_col = 'district')
    return assembly_districts, congressional_districts, senate_districts


## function to get concated df of district levels
def get_district_level_data(df_ct_district, gdf):
    '''
    for district levels: Congressional, Sentate, Assembley and Caltranse
    function gets shapes of various district levels and puts them into one dataframe. 
    
    '''
    # read in district levels
    assembly_districts, congressional_districts, senate_districts = read_in_district_shapes(gdf)
    
    #clean by_dist dataframe to match other district dfs
    ct_districts = df_ct_district>>select(_.DISTRICT, _.geometry, _.a2_ct_dist, _.Application, _.Funded, _.Total, _['Success Rate']) 
    ct_districts = ct_districts.rename(columns={'DISTRICT':'district','a2_ct_dist':'dist'})
    ct_districts = ct_districts.astype({"district":str,"dist": str, 
                                    "Application": float, "Funded": float, "Total": float})
    
    #add district type col
    ct_districts['dist_type'] = 'Caltrans District'
    assembly_districts['dist_type'] = 'Assembly District'
    senate_districts['dist_type'] = 'Senate District'
    congressional_districts['dist_type'] = 'Congressional District'
    
    districts = pd.concat([assembly_districts,  senate_districts, congressional_districts, ct_districts])
    
    districts = districts.rename(columns= {'district':'District',
                                       'population':'Population',
                                       'dist':'dist',
                                       'dist_type':'District Type'})
    
    return districts


def add_dropdown_map(districts_df):
    '''
    this function requires the function ______ to be ran 
    
    districts_df needs to be a district level df (Assembly, Senate, Congressional and CT)
    
    code help: ### Using Altair: https://altair-viz.github.io/gallery/multiple_interactions.html
    '''

    #identify options
    dist_options = ['Assembly District','Congressional District','Senate District','Caltrans District']
    
    #bind and add dropdown option
    dist_dropdown = alt.binding_select(options=dist_options)
    dist_select = alt.selection_single(fields=['District Type'], bind=dist_dropdown, name = 'District Type')
    
    
    #add base map
    base1 = (alt.Chart(districts_df).mark_geoshape().encode(
    color=alt.Color("Total",scale=alt.Scale(range=cp.CALITP_SEQUENTIAL_COLORS)),
    tooltip=['District Type', 'District', 'Total']))
    
    #add in selections
    filter_by_dist = base1.add_selection(dist_select).transform_filter(dist_select)
    
    #return map
    return filter_by_dist

def map_districts(df, 
                 district_type= '',
                 district_type_col: list = [],
                 mapping_col: list = [],
                 tool_tip_cols: list = []):
    
    df_subset =(df>>filter(_[district_type_col]== district_type))
    
    df_map = df_subset.explore(mapping_col,
                               cmap='Oranges',
                               legend=True,
                               legend_kwds=dict(colorbar=True),
                               tooltip= tool_tip_cols, 
                               name=(f"Number of Applications by {(district_type)}"))
    
    return df_map

def map_couty_proj(df):
    ## rename cols
    df = df.rename(columns={'imp_agency_name_new':'Implementing Agency Name',
                                'a2_county':'County',
                               'a2_info_proj_name':'Project Name',
                               'total_atp_$':'Total Funds',
                               'project_app_id':'Project ID'})
    
    #get list of unique counties
    county_list = list(df['County'].unique())

    #get dropdown menu
    county_dropdown = alt.binding_select(options=county_list)
    county_select = alt.selection_single(fields=['County'], bind=county_dropdown, name = 'County Name')
    
    #county bar chart 
    county_charts = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=('Total Funds'),
            y=('Implementing Agency Name:N'),
            color=alt.Color('Project Name', scale=alt.Scale(range=cp.CALITP_DIVERGING_COLORS), legend = None),
            tooltip=['Project Name','Total Funds'])
        .properties(title="Total ATP Funds by Project")
        .add_selection(county_select)
        .transform_filter(county_select))
    
    return county_charts

def success_rate_by_dist(df, agency_name_col, district):
    
    successes = (df>>group_by(_.awarded)>>count(_[agency_name_col]))>>spread("awarded", "n")>>arrange(-_.Y)
    successes['total'] = (successes['N'] + successes['Y'])
    successes['success_rate'] = (successes['Y']/successes['total'])
    
    successes = successes.rename(columns={"N":"Projects Not Funded",
                                    "Y":"Funded Projects", "total":"Total Applications"})
    
    #successes_top = successes>>filter(_.success_rate>0)
    #successes_top['success_rate'] = successes_top['success_rate'].transform(lambda x: '{:,.2%}'.format(x))
    successes['success_rate'] = successes['success_rate'].transform(lambda x: '{:,.2%}'.format(x))
    
    display(HTML(f"</br><h4> Success Rates for Agencies in District {district} </h4>"))
    display(HTML(_dla_utils.pretify_tables(successes>>select(_[agency_name_col], _['Total Applications'], _.success_rate))))


#district map
def get_district_map(gdf, district):
    df = gdf>>filter(_.a2_ct_dist==district)
    
    df =df.rename(columns = {"a2_ct_dist":"District", "awarded":"Awarded",
                             "a2_info_proj_name":"Project Name", "a3_proj_type":"Project Type",
                             "imp_agency_name_new":"Implementing Agency"})
    
    df['Awarded'] = df['Awarded'].map({'N': 'Not Funded', 'Y':'Funded'})
    
    dist_map = (df.explore(column="Awarded", 
            cmap= 'tab20b',
            marker_kwds=dict(radius=5),
            legend=True,
            legend_kwds=dict(colorbar=True),
            highlight= True,
            tooltip=["Implementing Agency", "Awarded", "Project Name", "Project Type"], 
            ))
    return dist_map
    

def map_dist_proj(df_funded, df_all, gdf, district):
    ## rename cols
    df_funded = df_funded.rename(columns={'imp_agency_name_new':'Implementing Agency Name',
                                'a2_county':'County',
                               'a2_info_proj_name':'Project Name',
                               'total_atp_$':'Total ATP Funds',
                               'project_app_id':'Project ID',
                           'total_project_cost':'Total Project Cost'})
    df_all = df_all.rename(columns={'imp_agency_name_new':'Implementing Agency Name',
                                'a2_county':'County',
                               'a2_info_proj_name':'Project Name',
                               'total_atp_$':'Total ATP Funds',
                               'project_app_id':'Project ID',
                           'total_project_cost':'Total Project Cost'})

    #filter
    df_funded = df_funded>>filter(_.a2_ct_dist==district) 
    df_all = df_all>>filter(_.a2_ct_dist==district) 

    display(HTML(f"<strong>Out of {len(df_all)} project applications in District {district}, "
             f"{len(df_funded)} projects were funded <br><br><br></strong>"))
    
    #district bar chart 
    dist_charts = (
        alt.Chart(df_funded)
        .mark_bar()
        .encode(
            x=('Total ATP Funds'),
            y=('Implementing Agency Name:N'),
            color=alt.Color('Project Name', scale=alt.Scale(range=cp.CALITP_DIVERGING_COLORS), legend = None),
            tooltip=['Project Name','Total ATP Funds'])
        .properties(title="Total ATP Funds by Project"))

    #district funded proj information
    quick_view = df_funded>>select(_['Implementing Agency Name'], _['Project Name'], 
                                   _['County'], _['Total ATP Funds'], _['Total Project Cost'])
    
    quick_view['Total ATP Funds'] = quick_view['Total ATP Funds'].map('$ {:0,.2f}'.format)
    quick_view['Total Project Cost'] = quick_view['Total Project Cost'].map('$ {:0,.2f}'.format)
    
    quick_view = _dla_utils.pretify_tables(quick_view)
        
    display(dist_charts)
    display(HTML("<h4>Funded Project Information</h4>"))
    display(HTML(quick_view))
    
    success_rate_table = success_rate_by_dist(df_all, 'Implementing Agency Name', district)
    
    district_map = get_district_map(gdf, district)
    display(district_map)
