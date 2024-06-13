### Cardinal Direction Links to address [this GH issue](https://github.com/cal-itp/data-analyses/pull/1124).
* [Slack thread](https://cal-itp.slack.com/archives/D02QC97TRA6/p1717516644055619)
* [This is where I need to input my cardinal direction work](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_funnel/schedule_stats_by_route_direction.py#L23)
* [This creates the dataframe that forms the basis of GTFS Digest](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/merge_data.py)
* [Mermaid 1](https://mermaid.live/view#pako:eNqNVttu2zgQ_RWCi8BZwAosqWpsPRRI6sh96AKLNFgEKxsFLY5sohKpklS2bpB_X1KUY_rW1A-2zDlz5sxwONQzLgQFnOIgCOZcM11BimYP2Rd0w0m10axQ6G_WQMU4oJkkjKs577AXF89zjhDjTKeoe0RooNdQwyBFgyVRMBj6q_8QyciyAjV4hRtTI1lN5OajqIS0fn_cJVmS3Wxdd4gH-KF3qNFodAy5FZKCPAeyGZyzKSgEp_s6suz67tbDaJCa7UHKshw484v9MV8vFxdzPudlJf4r1kRq9PneAYqKKDWFEkkoNCpZVaV9pgf2gsmigi2i03CCIeoB2SQb3UU2pIWodrmSpFkjxfjKkFCycYZtDgjd5KpYA20rWKAg-IBuwlyKVkNAmeVlgi88sINEuWhAEi3kvu3KGuNcadEsthLs5zZ_gjWzWTRCMcupXLDbMFewqoFrpBoAqnY-ByKCHrfPG_Ys5DJXa9JAYEOjHuqR2eWv2-XFn2ma9lX1uUhH9jEkOXky2a3gbU2WtiFMLgxjtws77AmVy8ucVBXSkjXnhN4_dFqRZjX4Qn22Za9zmftopO1R-j0hhSmXTa0mOw0eLvxWnwld9KGLgxKhN_ftK6OethOa6KXpO0JP1CREZ_XQXg89u2X2jJsxQ4Nal0XxqkbB9xZ48Ua9nDTT0G1tp8mbyS5852hve9TB_vTUfimm1uFJof3zOP3VeZw6SOSOnPsTH9D4DeXL69FbBcDp0dSwTqZkkoHyhWYnFHnWjjgL81pwvQ4DM3NCJ-3etJ0mdqpS9hNc8RAn5zq9J4ocUeQT-fHuHSw-FLWvftdMlJioZnyhjvZkU85OzbeZm2-zMLcH2ApBZLWSsLJ8Wji6TsssPvaKjmr2-wTdSl9Or8NcWVxFPr0qRo0U5iqAo8RObXBH4Wf-eHAbPPbd59Xh0Rn27gAvAkJ4iGuQNWHUvER09_ocd_f9HKfmkUJJ2krPsbkcDZS0WnzZ8AKnWrYwxCbcao3TklTK_Gsbu11TRozeegsBykzYv9xbSveyMsQN4Th9xj9wGkTx5CpJkjhOkvej8WiSDPEGp-G75CpKovA6CZPrSTKJXob4pxCGdHQ1GU_exeNkEsfj6_ejeNzR_dsZOx0v_wPlerTj)
* [Mermaid 2](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_funnel/mermaid.md)

### 6/12/2024
* [Issue 1135](https://github.com/cal-itp/data-analyses/issues/1135)
* Notebook: `gtfs_digest/17_cardinal_dir_pipeline`.
* I have edited `gtfs_funnel/schedule_stats_by_route_direction` and tested the dates April 15 and 16, 2024.
    * This is a different branch than the one I used to run the open_data/HQTA/etc stuff.
    * I saved out all my results with AH Testing
* I broke apart `gtfs_digest/merge_data`.
    * I used `concatenate_schedule_by_route_direction` to stack 4/15 and 4/16 together.
    * The dataframe from `concatenate_speeds_by_route_direction` and `concatenate_rt_vs_schedule_by_route_direction` and `concatenate_speeds_by_route_direction` don't have `stop_primary_direction`. Is that ok?
        * Yes that's ok. Cardinal_direction is derived from `schedule_only`.
        * Any row that is only found in `vehicle_positions` won't have cardinal directions.
    * Having issues with `merge_in_standardized_route_names` because the test dates I chose  don't have corresponding service_dates in the standardized routes dataframe. I think this should be ok.
        * Go to `gtfs_funnel/clean_route_naming` and update the `analysis_date` list.
        * The dataframe with standardized route names should have all dates to correspond. 
* I deleted out unknowns, so every row has an actual direction.
<s>* Now what? 
    * I think I'm done with the second checkbox in Issue #1135.
    * I am not sure if I should run it on a few dates without my `testing` string attached? Then proceed to backfilling all the dates?
    * " if digest needs to create new datasets in a different grain, do so"? I don't think I need a new dataset in a different grain? How would I know for certain?</s>
    * Amanda: discovered that there are many nans after doing `.value_counts(dropna=False`). Need to go back and fix this.
        * Don't replace missing data until the very last step. 
        * Figure out what's going on and why there are so many unpopulated rows, especially the rows that don't have `sched_only`, `sched_vp`, and `vp_only` values. 
        * Rename `stop_primary_direction` to `route_primary_direction`
        * Once I'm done: go into `gtfs_funnel/MakeFile` and write. Run this part of the Makefile to make changes to all of the files for all of the dates. Then go to `gtfs_digest/merge_data`. Delete the lines below after I'm done.
        `cardinal_dir_changes: 
         --- python schedule_stats_by_route
         --- python clean_route_naming`
<s>* Makefile and installing requirements for gtfs digest?
    * Find those versions and pin it down. 
    * Maybe it should be moved to _shared_utils/requirements
    * Amanda: done with this, found the latest version of `altair` and `altair_transform` that I download and update.</s>
* Tiffany ran May later because LA Metro was missing.
    * June's data will be run tomorrow.
    * Data for the most current month is run on the 2nd Thursday of each month. 
    * There are certain areas  of the `gtfs_funnel/MAKE_FILE` when you can actually open up a notebook and work. VP stuff gets close to the limit, but schedule takes a lot less memory.
    
### 6/7/2024
* I have added my work into the right file and it runs pretty fast. However, now that I'm done with this step I have no idea what to do. 
* Questions for Tiffany about adding Cardinal Direction into the pipeline
> so i would definitely stand up a test portfolio + scripts that generate the time-series data. you will need to cover the complete pipeline:
most upstream step (somewhere in gtfs_funnel)
data processing (situate your steps somewhere, either in gtfs_funnel, but it may be elsewhere, and you'd want to slightly programmatically rename the file, maybe just +_test so you can grab the newly processed data easily through each stage)
  * What does "upstream step" mean?
  * What's the difference between all your work in `data-analyses/gtfs_funnel` and `gtfs_funnel/merge_data.py`. 
> every output needs a new suffix while you're testing. and then you point your input to that.
otherwise, you'd have to run all the dates (with schedule data, that's not a big deal, but 2 min here and there over 30-40 dates does add up).
stop_times -> aggregate to route_dir -> schedule_data_for_date1 (AH note: this is how the dataset is constructed for one date)
time-series = schedule_data_for_date1 , schedule_data_for_date2, schedule_data_for_date3 (AH note: )
if you added a column in date3, that column isn't present in date1, date2, so time_series_utils would error because it's looking for all the columns you listed
digest is displaying whatever is concatenated in time_series_utils
* What do you mean by output and input? When you say input are you talking about the dataframes I add to make this into a time series?

> if you want to test a smaller set of dates to make sure it's working, i would start adding suffixes to everything.
stop_times -> aggregate to route_dir -> schedule_data_for_date1_test
time-series = schedule_data_for_date1_test , schedule_data_for_date2_test, schedule_data_for_date3_test
digest can pull from a test file with just 3 dates to see if it has everything.
once you're happy with it, you have to run all the scripts that are affected schedule_stats_by_route_direction, anything downstream that uses that intermediate output, all the way through to digest for all the dates in rt_dates.y2023_dates and rt_dates.y2024_dates
* Where do I find `aggregate to route_dir`? 
* `Test file`: is this like saving the outputs into GCS like 'may_2024_test.parquet'?
* How do I know which scripts are affected by `schedule_stats_by_route_direction`?

> yes, but before the meeting, can you go through and identify in a notebook or text file what you think is the "batch" from upstream to downstream?
script 1, input file is data1, output file is data2
script 2, input file is data2, output is data3
and so forth, until you reach the digest, wherever you think the end script is
* What is the "batch"?
* What do you mean by "reach the digest?"
* [Run everything using this Makefile](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_funnel/Makefile)
* [schedule_stats_by_route_direction.py](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_funnel/schedule_stats_by_route_direction.py#L15)
    * `assemble_scheduled_trip_metrics` 
        * Input:
            * `import_scheduled_trips`
            * `import stop times direction`
        * Output:
            *GTFS_DATA_DICT.rt_vs_schedule_tables.sched_trip_metrics
    * `schedule_metrics_by_route_direction`
        * Input:
            * End result from `assemble_scheduled_trip_metrics` (for one date)
        * Output
            * GTFS_DATA_DICT.rt_vs_schedule_tables.sched_route_direction_metrics
    * After this goes [here](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_digest/merge_data.py#L25)

* What I need to change
    * [Here](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_funnel/schedule_stats_by_route_direction.py#L118C19-L120) add `test` behind the name and [here](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_funnel/schedule_stats_by_route_direction.py#L122) change the `analysis_date_list` to only a few dates.
    * [Here](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_digest/merge_data.py#L25) add _test 
    * [Edit here](https://github.com/cal-itp/data-analyses/blob/ah_gtfs_portfolio/gtfs_digest/merge_data.py#L212) so I only run a few dates.
* [Only run above here](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/merge_data.py#L208-L268)
* Once I'm happy with this, there's a GCS Public Bucket. That's the very, very late step.
    * After I deploy everything. 
* 
#### Terms
* Batch
* Upstream: the least processed
    * Warehouse: could be unzipped files.
    * For me, it's something downloaded from the mart.
    * WE process these unprocessed files with scripts. 
    * Called by helper functions.
* Downstream: the most final product.
    * The most downstream is the digest. 
    * Whatever is done in the GTFS Digest. 
* Digest
    * 
* Funnel 
    * When Tiffany is downloading stuff.
    * Funneling the least processed tables through various scripts to trasnform.
    * There are 5 tables that are transformed repeatedly into different products. 
    * Everything that is in `gtfs_funnel`.
    * 3 big workstreams: 
        * RT Segment Speeds
        * Schedule
        * RT vs Schedule. 
        * Schedule is also processed in GTFS Funnel. 
    * Schedule data informs a lot of the interpreation of the vehicle positions
        * We don't know anything about vehicle positions, what route, shape, direction it is. This data is all found in Schedule data that we connect with GTFS Key.
        * Trips is the main table of the Schedule universe. 
* Helper functions just pull dataframes that are downloaded.
* Downloading happens in another step.
* Tiffany runs the MAKEFILE in gtfs_funnel.
* Then Tiffany goes to various other folders and runs everything (HQTA, RT Segment Speeds, etc)
* Everything at the end goes into the GTFS Digest.
* GTFS Digest is trying to represent everything through the grain of route-direction.
* Every month Tiffany runs one day.
* Batch: a combination of various scripts that are run together.
* Look at the MAKEFILE and compare it to Mermaid. 
* Digest: simply a compilation by dates, collects the columns TIffany wants
    * Select the columns they want.
* Not all the scripts in the Makefile need to be matched.
* I can duplicate the files. 