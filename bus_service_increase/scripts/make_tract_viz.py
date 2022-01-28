import branca
import pandas as pd

import prep_data
import utils
from setup_tract_charts import *
from setup_tract_maps import *
from shared_utils import geography_utils, map_utils
from shared_utils import calitp_color_palette as cp

#----------------------------------------------------------------#
## Functions to wrangle data for charts
#----------------------------------------------------------------#
def import_processed_data():
    df = prep_data.generate_stop_times_tract_data()
    df = df.assign(
        geometry = df.geometry.simplify(tolerance=0.005)
    )
    # Address small numbers issue
    df = df[df.Population > 10].reset_index(drop=True)
    
    return df

    
# Further subset data
# How to write a function that can handle multiple districts, MPOs, counties?
# county is easy bc tract has that attached
'''
def attach_crosswalk(df, crosswalk, geography="county"):
    if geography=="county":
        # do this
    elif geography=="district":
        # do this
    elif geography=="MPO":
    else:
        # handle all other cases...how?
'''
# After subsetting data, generate more summary stats
# Can aggregate by equity, by equity-density, by density, etc

def aggregate_generate_stats(df, group_cols):
    # After subset
    t1 = geography_utils.aggregate_by_geography(
        df, 
        group_cols = group_cols, 
        sum_cols = ["stop_id", "itp_id", "Population", 
                    "num_jobs", "num_pop_jobs", "num_arrivals"], 
        count_cols = ["Tract"], 
    ).astype("int").sort_values(group_cols)
    
    DENOM = 1_000
    t1 = t1.assign(
        arrivals_per_1k_pj = (t1.num_arrivals / t1.num_pop_jobs) * DENOM,
        stops_per_1k_pj = (t1.stop_id / t1.num_pop_jobs) * DENOM,
    )  
    
    return t1


# Need to pivot df first for heatmaps
def pivot_get_counts(df, index_col, group_col, value_col):
    df2 = df.pivot(index = index_col, 
                   columns = group_col, 
                   values = value_col)
    return df2

#----------------------------------------------------------------#
## Put all the chart-making into 1 function
# Allow for IMG_PATH to change if there are subsets of CA
# save to another sub-folder, same naming convention
#----------------------------------------------------------------#
def create_charts(df, CHART_IMG_PATH):
    
    ## Create 3 sets of aggregated datasets to use for charts
    # (1) By popjobdensity - equity
    by_density_equity = aggregate_generate_stats(
        df, 
        group_cols = ["popjobdensity_group", "equity_group"])

    by_density_equity = by_density_equity.assign(
        popjobdensity_group = by_density_equity.popjobdensity_group.map(LEGEND_LABELS)
    )
    
    # (2) By popjobdensity
    by_density = aggregate_generate_stats(
        df, 
        group_cols = ["popjobdensity_group"])

    # (3) By equity
    by_equity = aggregate_generate_stats(
        df, 
        group_cols = ["equity_group"])
    
    
    ## Make bar charts
    y_col = "arrivals_per_1k_pj"
    DENSITY_MAX = by_density[y_col].max()
    EQUITY_MAX = by_equity[y_col].max()
    Y_MAX = max(DENSITY_MAX, EQUITY_MAX) + 10
    
    # For any subsets of data...change the img path to be in another sub-folder
    bar_chart(by_density_equity, 
              x_col="popjobdensity_group", y_col=y_col, color_col="popjobdensity_group",
              Y_MAX=Y_MAX, chart_title="Arrivals per 1k by Pop / Job Density", 
              IMG_PATH = CHART_IMG_PATH)
    
    bar_chart(by_equity.assign(
        equity_group = by_equity.equity_group.map(LEGEND_LABELS)), 
              x_col="equity_group", y_col=y_col, color_col="equity_group", 
              Y_MAX=Y_MAX, chart_title="Arrivals per 1k by CalEnviroScreen",
              IMG_PATH = CHART_IMG_PATH
             )
    
    grouped_bar_chart(by_density_equity[by_density_equity.equity_group > 0],
                      x_col="popjobdensity_group", y_col=y_col, 
                      color_col="popjobdensity_group", grouped_col="equity_group", 
                      Y_MAX=Y_MAX, chart_title="Arrivals per 1k by CalEnviroScreen",
                      IMG_PATH = CHART_IMG_PATH
                     )
    
    ## Make heatmaps
    LOOP_DICT = {
        "Tract": "Tracts",
        "arrivals_per_1k_pj": "Arrivals per 1k",
    } 
    

    for key, value in LOOP_DICT.items():
        subset_df = by_density_equity[by_density_equity.equity_group > 0]
    
        index_col = "popjobdensity_group"
        group_col = "equity_group"

        pivot_df = pivot_get_counts(subset_df, index_col, group_col, key)
        pivot_df = pivot_df.reindex(index=LEGEND_ORDER)
    
        #display(pivot_df.sort_index(axis=0, ascending=True))
        #display(pivot_df.reindex(index=LEGEND_ORDER))

        # Not sure how to insert custom cmap and set boundaries
        # https://stackoverflow.com/questions/50192121/custom-color-palette-intervals-in-seaborn-heatmap
        heatmap = make_heatmap(pivot_df, 
                     chart_title=(f"{value} by CalEnviroScreen" "\n and Density Levels"),
                     xtitle=f"{labeling(index_col)}", ytitle=f"{labeling(group_col)}"   
                     )

        plt.gcf().set_size_inches(8, 8)
        plt.savefig(f'{CHART_IMG_PATH}{key}.png') #figsize=(4, 4), dpi=300)
        plt.close()
        
        
    ## Make scatterplot
    for x_col in ["popjobs_sq_mi", "pop_sq_mi", "jobs_sq_mi"]:
        
        y_col = "arrivals_per_1k_pj"
        group_col = "equity_group"
        tooltip=['Tract', 'ZIP', 'County', 'City', 
                 'pop_sq_mi', 'jobs_sq_mi', 'popjobs_sq_mi',
                 'arrivals_per_1k_pj', 'equity_group']
        
        scatter_title = f"{x_col.replace('_sq_mi', 'Density').replace('popjobs', 'Pop / Job').title()}"
        
        # Scatterplot of popjob or pop or job density vs service density
        c = make_scatterplot(
            df[(df.equity_group > 0)], 
            f"{x_col}:Q", f"{y_col}:Q", f"{group_col}:N", 
            tooltip, chart_title=f"{scatter_title} vs Service Density")
        
        c.save(f"{CHART_IMG_PATH}scatter_{x_col.replace('_sq_mi', 'density')}_servicedensity.html")
    

    
def create_maps(df, CHART_IMG_PATH):
    
    ## Set folium variables common across all maps
    FIG_HEIGHT = 900
    FIG_WIDTH = 700
    
    colorscale = branca.colormap.StepColormap(
                    colors=[cp.CALITP_CATEGORY_BRIGHT_COLORS[0], 
                            cp.CALITP_CATEGORY_BRIGHT_COLORS[1],
                            cp.CALITP_CATEGORY_BRIGHT_COLORS[2]], 
                    vmin=1, vmax=3,
    )
    
    ## folium map for arrivals per 1k by census tract
    # Subset and loop through equity groups low/med/high
    EQUITY_GROUPS = {
        1: "Low", 
        2: "Med",
        3: "High",
    }
    
    plot_col = "arrivals_per_1k_pj"
    arrivals_col = "arrivals_group_pj"
    # For indexing cut-off, we need 1 less than the number of groups
    # It goes from [vmin, cut1], [cut1, cut2], [cut2, cut3]
    # Not sure where the open parantheses are [) or (]?
    # Use a function. Right now it's plotting arrivals_per_1k_pj, 
    # but num_arrivals would need different cut-offs, bc scale of the values differs
    colormap_cutoff, MIN_VALUE, MAX_VALUE = grab_legend_thresholds(
        df, plot_col, arrivals_col)
    
    gray = "#8D9191"
    light_green = cp.CALITP_CATEGORY_BRIGHT_COLORS[3]
    light_blue =  cp.CALITP_CATEGORY_BRIGHT_COLORS[0]
    navy = cp.CALITP_SEQUENTIAL_COLORS[4]
    
    colorscale2 = branca.colormap.StepColormap(
                    colors=[
                        #gray, 
                        light_green, light_blue, navy], 
                    index=colormap_cutoff,
                    vmin=MIN_VALUE, vmax=colormap_cutoff[2] + 100,
    )
    
    popup_dict = {
        "Tract": "Census Tract",
        "ZIP": "Zip Code",
        "County": "County",
        "City": "City",
        "equity_group": "CalEnviroScreen Group (3 is high need)", 
        "popjobdensity_group": "Pop & Job Density Group (3 is densest)",
        "num_arrivals": "# Daily Bus Arrivals",
        "stop_id": "# Bus Stops",
        "itp_id": "# Operators",
        "arrivals_per_1k_pj": "Arrivals per 1k",
        "arrivals_group_pj": "Arrivals Group (3 is highest)",
    }
        
    for i, group_name in EQUITY_GROUPS.items():        
        fig = map_utils.make_folium_choropleth_map(
            df[df.equity_group==1], plot_col = plot_col, 
            popup_dict = popup_dict, tooltip_dict = popup_dict, 
            colorscale = colorscale2, fig_width = FIG_WIDTH, fig_height = FIG_HEIGHT, 
            zoom = map_utils.REGION_CENTROIDS["CA"]["zoom"], 
            centroid = map_utils.REGION_CENTROIDS["CA"]["centroid"],
            title=f"Bus Service per 1k for {group_name}-Need CalEnviroScreen Tracts")
                                   
        fig.save(f"{CHART_IMG_PATH}arrivals_pc_{group_name.lower()}.html")
    
    
    ## folium map for where there is zero service
    # Map for where there is zero service
    #https://notebook.community/racu10/emapy/notebooks/Colormaps
    popup_dict = {
        "Tract": "Census Tract",
        "ZIP": "Zip Code",
        "County": "County",
        "City": "City",
        "equity_group": "CalEnviroScreen Group (3 is worst)", 
        "popjobdensity_group": "Job/Population Density Group (3 is densest)",
    }

    plot_col = "popjobdensity_group"

    fig = map_utils.make_folium_choropleth_map(
        df[(df.equity_group==3) & (df.stop_id==0)], plot_col = plot_col, 
        popup_dict = popup_dict, tooltip_dict = popup_dict, 
        colorscale = colorscale, fig_width = FIG_WIDTH, fig_height = FIG_HEIGHT, 
        zoom = map_utils.REGION_CENTROIDS["CA"]["zoom"], 
        centroid = map_utils.REGION_CENTROIDS["CA"]["centroid"],
        title=f"Zero Bus Service for High-Need CalEnviroScreen Tracts"
    )
    fig.save(f"{CHART_IMG_PATH}zero_service_high.html")
    
    
    ## folium map for opportunity tracts
    # Grab the ones with density above 75th percentile
    # and in the lower 25th percentile for service density
    p75 = df.popjobs_sq_mi.quantile(0.75)
    p25 = df.arrivals_per_1k_pj.quantile(0.25)

    df = df.assign(
        flag_me = df.apply(lambda x: (x.popjobs_sq_mi > p75) and 
                           (x.arrivals_per_1k_pj < p25), axis=1)
    )
    
    popup_dict = {
        "Tract": "Census Tract",
        "ZIP": "Zip Code",
        "County": "County",
        "City": "City",
        "equity_group": "CalEnviroScreen Group (3 is high need)", 
        "Population": "Population",
        "num_jobs": "Jobs",
        "popjobs_sq_mi": "Pop & Job Density Group (3 is densest)",
    }

    plot_col = "equity_group"
    arrivals_col = "arrivals_group_pj"

    fig = map_utils.make_folium_choropleth_map(
        df[df.flag_me==1], plot_col = plot_col, 
        popup_dict = popup_dict, tooltip_dict = popup_dict, 
        colorscale = colorscale, 
        fig_width = FIG_WIDTH, fig_height = FIG_HEIGHT, 
        zoom = map_utils.REGION_CENTROIDS["CA"]["zoom"], 
        centroid = map_utils.REGION_CENTROIDS["CA"]["centroid"],
        title="Opportunity Tracts by CalEnviroScreen Need")
    
    fig.save(f"{CHART_IMG_PATH}opportunity_tracts.html")


df = import_processed_data()
# additional function to subset
create_charts(df, CHART_IMG_PATH="./test_img/")
create_maps(df, CHART_IMG_PATH = "./test_img/")