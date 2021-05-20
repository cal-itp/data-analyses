NOTEBOOKS=1a_calendar_dates 1b_agency_routes 1c_stops_and_times 2a_service_hours
REPORTS=$(addsuffix /readme.md, $(NOTEBOOKS))

all: $(REPORTS)

%/readme.md: ipynb/%.ipynb
	mkdir -p $*
	jupyter nbconvert --template-file _template.tpl --to markdown --output ../$@ $<
