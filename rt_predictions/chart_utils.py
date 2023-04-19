import altair as alt
from shared_utils import calitp_color_palette as cp

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