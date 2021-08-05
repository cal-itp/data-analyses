# +
from calitp.tables import tbl
from siuba import *

import pandas as pd

REPORT_PUBLISH_DATE = "2021-07-01"
REPORT_LINK_BASE = "https://reports.calitp.org/gtfs_schedule/2021/07"
DEV_LINK_BASE = "https://development-build--cal-itp-reports.netlify.app/gtfs_schedule/2021/07"

# +
df_crm_emails = pd.read_csv(
    "https://docs.google.com/spreadsheets"
    "/d/1AHFa5SKcEn7im374mPwULFCj7VlFchQr/export?gid=289636985&format=csv",
    skiprows=1,
)

df_crm_emails.columns = df_crm_emails.columns.str.lower()

# +
tbl.gtfs_schedule.feed_info() >> count(missing_email = _.feed_contact_email.isna())

tbl_report_emails = (
    tbl.views.reports_gtfs_schedule_index()
    >> left_join(
        _,
        tbl.gtfs_schedule.feed_info()
        >> select(_.startswith("calitp"), _.feed_contact_email),
        ["calitp_itp_id", "calitp_url_number"]
    )
    # NOTE: reports currently only use url number = 0, so we remove any others --
    >> filter(_.publish_date == REPORT_PUBLISH_DATE,  _.calitp_url_number == 0)
    >> select(_.startswith("calitp"), _.agency_name, _.use_for_report, _.feed_contact_email)
)

report_emails = tbl_report_emails >> collect()
# -

report_crm_emails = (
    report_emails
    >> full_join(
        _,
        df_crm_emails
        >> filter(_.itp_id.notna())
        >> mutate(itp_id=_.itp_id.astype(int)),
        {"calitp_itp_id": "itp_id"},
    )
)

report_crm_emails_inner = (
    report_crm_emails
    >> filter(_.calitp_itp_id.notna(), _.itp_id.notna())
    >> select(-_.itp_id)
    >> mutate(calitp_itp_id=_.calitp_itp_id.astype(int))
)


# +
final_pipeline_emails = (report_emails
   >> transmute(_.calitp_itp_id, _.calitp_url_number, email = _.feed_contact_email, origin = "feed_info.txt")
   >> filter(_.email.notna())
)

final_crm_emails = (
    report_crm_emails_inner
    >> transmute(_.calitp_itp_id, _.calitp_url_number, email = _["main email"], origin = "CRM")
    >> filter(_.email.notna())
)

final_all_emails = (
    pd.concat([final_pipeline_emails, final_crm_emails], ignore_index = True)
    >> mutate(
        calitp_itp_id = _.calitp_itp_id.astype(int),
        calitp_url_number = _.calitp_url_number.astype(int),
        report_url = REPORT_LINK_BASE + "/" + _.calitp_itp_id.astype(str) + "/",
        dev_url = DEV_LINK_BASE + "/" + _.calitp_itp_id.astype(str) + "/",
    )
)
# -

final_all_emails.to_csv(f"report_emails_{REPORT_PUBLISH_DATE}.csv")
