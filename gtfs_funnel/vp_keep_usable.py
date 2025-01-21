condensed = vp_transform.condense_point_geom_to_line(
        vp_gdf,
        group_cols = ["trip_instance_key"],
        sort_cols = ["trip_instance_key", "vp_idx"], 
        array_cols = ["vp_idx", "geometry"]        
    )
    
    vp_direction_series = []
    
    for row in vp_condensed.itertuples():
        vp_geom = np.array(getattr(row, "geometry"))
        next_vp_geom = vp_geom[1:]
    
        vp_direction = np.array(
            ["Unknown"] + 
            [rt_utils.primary_cardinal_direction(prior_vp, current_vp)
            for prior_vp, current_vp in zip(vp_geom, next_vp_geom)
        ])
        
        vp_direction_series.append(vp_direction)
    
    keep_cols = ["vp_idx", "vp_primary_direction"]
    
    vp_condensed = vp_condensed.assign(
        vp_primary_direction = vp_direction_series
    )[keep_cols].pipe(
        vp_transform.explode_arrays, 
        array_cols = keep_cols
    )
    
    vp_condensed.to_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet"
    )
               
    return 
    

if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    LOG_FILE = "./logs/vp_preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()
  
        pare_down_to_valid_trips(
            analysis_date,
            GTFS_DATA_DICT
        )
        
        merge_in_vp_direction(
            analysis_date,
            GTFS_DATA_DICT
        )
        
        end = datetime.datetime.now()
        logger.info(
            f"{analysis_date}: pare down vp, add direction execution time: "
            f"{end - start}"
        )
        