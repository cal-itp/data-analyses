GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"


"""
functions for the new transit metrics portfolio
"""
import pandas as pd
import altair as alt
from calitp_data_analysis.tables import tbls
from siuba import _, collect, count, filter, show_query, select
import sys
sys.path.append("../")  # up one level
from update_vars import NTD_MODES, NTD_TOS


def make_new_transit_metrics_data():
    year_list=["2018","2019","2020","2021","2022","2023"]
    col_list=['agency_name',
          'agency_status',
          'city','ntd_id',
          'reporter_type',
          'reporting_module',
          'state',
          'mode',
          'service',
          'primary_uza_name',
          'year',]
    
    # get opex data
    op_total = (
        tbls.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_opexp_total()
        >> select(
            _.agency_name,
            _.agency_status,
            _.city,
            _.mode,
            _.service,
            _.ntd_id,
            _.reporter_type,
            _.reporting_module,
            _.state,
            _.primary_uza_name,
            _.year,
            _.opexp_total,
        )
        >> filter(
            _.state == "CA",
            _.primary_uza_name.str.contains(", CA"),
            _.year.isin(year_list),
            _.opexp_total.notna(),
        )
        >> collect()
    )
    
    #get mode data
    mode_upt = (
        tbls.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt()
        >> select(
            _.agency_name,
            _.agency_status,
            _.city,
            _.mode,
            _.service,
            _.ntd_id,
            _.reporter_type,
            _.reporting_module,
            _.state,
            _.primary_uza_name,
            _.year,
            _.upt,
        )
        >> filter(_.state == "CA",
                  _.primary_uza_name.str.contains(", CA"),
                  _.year.isin(year_list),
                  _.upt.notna()
                 )
        >> collect()
    )
    
    #get vrh data
    mode_vrh = (
        tbls.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_vrh()
        >> select(
            _.agency_name,
            _.agency_status,
            _.city,
            _.mode,
            _.service,
            _.ntd_id,
            _.reporter_type,
            _.reporting_module,
            _.state,
            _.primary_uza_name,
            _.year,
            _.vrh,
        )
        >> filter(_.state == "CA",
                  _.primary_uza_name.str.contains(", CA"),
                  _.year.isin(year_list),
                  _.vrh.notna()
                 )
        >> collect()
    )
    
    # get vrm data
    mode_vrm = (
        tbls.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_vrm()
        >> select(
            _.agency_name,
            _.agency_status,
            _.city,
            _.mode,
            _.service,
            _.ntd_id,
            _.reporter_type,
            _.reporting_module,
            _.state,
            _.primary_uza_name,
            _.year,
            _.vrm,
        )
        >> filter(_.state == "CA",
                  _.primary_uza_name.str.contains(", CA"),
                  _.year.isin(year_list),
                  _.vrm.notna()
                 )
        >> collect()
    )
    
    # merge upt to vrh
    merge_upt_vrh = mode_upt.merge(
        mode_vrh,
        on= col_list,
        how="left",
        indicator=True,
    )
    
    #then merge vrm to previous
    merge_upt_vrh_vrm = merge_upt_vrh.drop(columns="_merge").merge(
        mode_vrm,
        on = col_list,
        how = "left",
        indicator=True
    )
    
    # merge in opex total
    merge_opex_upt_vrm_vrh = merge_upt_vrh_vrm.drop(columns="_merge").merge(
        op_total,
        on= col_list,
        how="left",
        indicator=True
    )
    merge_opex_upt_vrm_vrh["opexp_total"] = merge_opex_upt_vrm_vrh["opexp_total"].fillna(0)
    
    # read in ntd id-to-RTPA xwalk
    xwalk_path = "ntd_id_rtpa_crosswalk_all_reporter_types.parquet"

    rtpa_ntd_xwalk = pd.read_parquet(f"{GCS_FILE_PATH}{xwalk_path}")
    
    # merge in xwalk
    merge_metrics_rtpa = merge_opex_upt_vrm_vrh.drop(columns="_merge").merge(
        rtpa_ntd_xwalk,
        on=[
            "ntd_id",
            "city",
            "state",
            "agency_name",
            "reporter_type",
            "agency_status"
        ],
        how="left",
        indicator=True
    )
    
    merge_metrics_rtpa["opexp_total"] = merge_metrics_rtpa["opexp_total"].astype("int64")
    merge_metrics_rtpa["mode"] = merge_metrics_rtpa["mode"].map(NTD_MODES)
    merge_metrics_rtpa["service"] = merge_metrics_rtpa["service"].map(NTD_TOS)
    
    return merge_metrics_rtpa


def sum_by_group(
    df: pd.DataFrame,
    group_cols: list) -> pd.DataFrame:
    """
    since data is now long to begin with, this replaces old sum_by_group, make_long and assemble_long_df functions.
    """
    grouped_df = df.groupby(group_cols+
                             ["year"]
                           ).agg({
        "upt":"sum",
        "vrm":"sum",
        "vrh":"sum",
        "opexp_total":"sum"
    }
    ).reset_index()
    
    calc_dict = {
    "opex_per_vrh": ("opexp_total", "vrh"),
    "opex_per_vrm": ("opexp_total", "vrm"),
    "upt_per_vrh": ("upt", "vrh"),
    "upt_per_vrm": ("upt", "vrm"),
    "opex_per_upt": ("opexp_total", "upt"),
    }

    for new_col, (num, dem) in calc_dict.items():
        grouped_df[new_col] = (
            grouped_df[num] / grouped_df[dem]
        ).round(2)
    
    return grouped_df  


def make_long(df: pd.DataFrame, group_cols: list, value_cols: list):
    """
    melts dataframes to get all the metrics into a single column for better charting
    """
    df_long = df[group_cols + ["year"] + value_cols].melt(
        id_vars = group_cols+ ["year"], 
        value_vars = value_cols,
    )
    
    return df_long


def make_scatter(data, x_ax, y_ax, chart_title, color=None, column_num=None, log_scale=None, lin_x_ax=None, lin_y_ax=None):
    """
    makes scatterplot from designated z and y axis.
    if scale is enabled, filters x and y axis cols for greater than zero to make log scale work
    """
    
    chart = (
        alt.Chart(data)
        .mark_point()
        .encode(
            x=alt.X(x_ax)
            , 
            y=alt.Y(y_ax)
            , 
            tooltip=[x_ax, y_ax, "agency_name","year"])
    )

    if color:
        chart = chart.encode(color=color)
    if column_num:
        chart = chart.encode(columns=col_num)
    if log_scale:
        filtered_df = data[(data[x_ax]>0) & (data[y_ax]>0)]
        chart = alt.Chart(filtered_df).mark_point().encode(
            x=alt.X(x_ax) if lin_x_ax else alt.X(x_ax).scale(type="log"),
            y=alt.Y(y_ax) if lin_y_ax else alt.Y(y_ax).scale(type="log"),
            color= color if color else None,
            tooltip=[x_ax, y_ax, "agency_name"]
        )
        
        excluded_count = len(data) - len(filtered_df)
        print(f"{excluded_count} rows with zero or negative values excluded due to log scale.")

    chart = chart.properties(
        title=chart_title, width=350, height=150
    ).interactive()

    return chart + chart.transform_regression(x_ax, y_ax).mark_line()


def make_line(
    df,
    x_col,
    y_col,
    color,
    facet,
    chart_title=None,
    ind_axis=None,
):
    """
    worked with melted dataframe wth a single value column.
    """
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(x_col),
            y=alt.Y(y_col),
            color=alt.Color(color),
            tooltip=[x_col, y_col,color]
        )
    ).properties(
        title=chart_title, width=350, height=150
    ).interactive()
    
    # median, horizontal bar
    rule = alt.Chart(df).mark_rule().encode(
        y=alt.Y('median(' + y_col + ')')
    )
    
    # data labels for line
    labels = alt.Chart(df).mark_text(align="right", dy=-10).encode(
        x=alt.X(x_col),
        y=alt.Y(y_col),
        text=alt.Text(y_col),
        color=alt.Color(color)
    )
    
    chart_w_rule = alt.layer(chart, rule, labels)
    
    chart_rule_facet = chart_w_rule.facet(
        facet=alt.Facet(
                facet,
        ),columns=3
               )
    
    if ind_axis:
        chart_rule_facet = chart_rule_facet.resolve_scale(
            #x="independent", 
            y="independent"
        )
        
    return chart_rule_facet

if __name__ == "__main__":
    df = make_new_transit_metrics_data()
    df.to_parquet(f"{GCS_FILE_PATH}raw_transit_performance_metrics_data.parquet")
