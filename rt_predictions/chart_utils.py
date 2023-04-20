import altair as alt
from shared_utils import calitp_color_palette as cp
import pandas as pd

def altair_dropdown(df, column_for_dropdown:str, title_of_dropdown:str):
    """
    Create a dropdown menu. Selects the first 
    operator as the default display option.
    """
    dropdown_list = df[column_for_dropdown].unique().tolist()
    initialize_first_op = sorted(dropdown_list)[0]
    input_dropdown = alt.binding_select(options=sorted(dropdown_list), name=title_of_dropdown)
    
    selection = alt.selection_single(
    name=title_of_dropdown,
    fields=[column_for_dropdown],
    bind=input_dropdown,
    init={column_for_dropdown: initialize_first_op},)
    
    return selection

def reverse_snakecase(df):
    """
    Clean up columns to remove underscores and spaces.
    """
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    return df

def chart_size(chart: alt.Chart) -> alt.Chart:
    """
    Resize charts.
    """
    chart = chart.properties(width=500, height=400)
    return chart

def describe_to_df(df, operator: str, metric_cols: list) -> pd.DataFrame:
    """
    Convert df.column.describe() to a 
    horizontally concatted dataframe.
    """
    # Filter for operator
    df = df[df._gtfs_dataset_name == operator].reset_index(drop=True)

    final = pd.DataFrame()

    for i in metric_cols:
        df2 = pd.DataFrame({i: df[i].describe()})
        final = pd.concat([final, df2], axis=1)

    final = final.reset_index().rename(columns={"index": "Measure"})

    final = reverse_snakecase(final)

    final.Measure = final.Measure.str.title()
    
    # https://stackoverflow.com/questions/59535426/can-you-change-the-caption-font-size-using-pandas-styling
    final = final.style.set_caption(f"Summary for {operator}").set_table_styles([{
    'selector': 'caption',
    'props': [
        ('color', 'black'),
        ('font-size', '16px')
    ]}]).format(precision=1)

    return final

def prep_df_for_chart(df, 
                      percentage_column: str, 
                      columns_to_round: list, 
                      columns_to_keep: list):
    """
    Clean up dataframe before creating charts. 
    Round certain columns, round it to one decimal place,
    sort by stop sequence, etc. 
    """

    df[percentage_column] = df[percentage_column] * 100

    df = df.sort_values(
        by=["_gtfs_dataset_name", "trip_id", "stop_sequence"]
    ).reset_index(drop=True)

    df = df[columns_to_keep]

    # Rounds down. 96 becomes 90.
    for i in columns_to_round:
        df[f"rounded_{i}"] = ((df[i] / 100) * 10).astype(int) * 10
    
    df = reverse_snakecase(df)

    df = df.round(1)

    return df

def scatter_plot(
    df,
    x_col: str,
    y_col: str,
    color_col: str,
    dropdown_col: str,
    dropdown_col_title: str,
):
    """
    Create scatterplot.
    """
    selection = altair_dropdown(df, dropdown_col, dropdown_col_title)

    chart = (
        alt.Chart(df)
        .mark_circle(size=250)
        .encode(
            x=alt.X(
                f"{x_col}:N",
                scale=alt.Scale(domain=df[x_col].unique().tolist()),
            ),
            y=alt.Y(y_col, scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                color_col,
                scale=alt.Scale(range=cp.CALITP_SEQUENTIAL_COLORS, domain=[0, 100]),
            ),
            tooltip=df.columns.to_list(),
        )
        .properties(title=f"Operator - Metric {y_col}")
        .interactive()
    )

    chart = chart_size(chart)
    chart = chart.add_selection(selection).transform_filter(selection)

    return chart

def scatter_plot_operator(
    df,
    operator,
    x_col: str,
    y_col: str,
    color_col: str,
    dropdown_col: str,
    dropdown_col_title: str,
):
    """
    Create scatterplot.
    """
    df = df[df['Gtfs Dataset Name'] == operator].reset_index(drop = True)
    
    operator_title = df['Gtfs Dataset Name'].iloc[0]
    
    selection = altair_dropdown(df, dropdown_col, dropdown_col_title)

    chart = (
        alt.Chart(df)
        .mark_circle(size=250)
        .encode(
            x=alt.X(f"{x_col}:Q",
                scale=alt.Scale(domain=[df[x_col].min(), df[x_col].max()]),
            ),
            y=alt.Y(y_col, scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                color_col,
                scale=alt.Scale(range=cp.CALITP_DIVERGING_COLORS, domain=[0, 100]),
            ),
            tooltip=df.columns.to_list(),
        )
        .properties(title=f"{operator_title} - Metric {y_col}")
        .interactive()
    )

    chart = chart_size(chart)
    chart = chart.add_selection(selection).transform_filter(selection)

    return chart