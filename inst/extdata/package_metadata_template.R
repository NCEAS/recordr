title <- "Recordr publish test"
creators <- data.frame(
    surname=c("Peter"),
    given=c("Slaughter"),
    email=c("slaughter@nceas.ucsb.edu"),
    stringsAsFactors=FALSE)
abstract <- "This is an example abstract for the data set. This is only a test."
methodDescription <- "This is the method description."
geo_coverage <- geoCoverage("Southern California", west=119.0, east=117, north=36, south=33)
temp_coverage <- temporalCoverage("2014", "2014")
TRUE
