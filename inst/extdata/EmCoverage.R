library(dataone)
# This test script can be used to test the 'record' method of the recordr package.
# This script downloads a dataset from DataONE and creates a local CSV file from
# this dataset.
# The provenance relationships that should be recorded are:
#     Endocladia_muricata.csv < wasGeneratedBy <- plotCoverages.R
#     plotCoverages.R <- used <- coverages_2001-2010.csv
#
# Here is a new comment
df <- read.csv(file = system.file("./extdata/coverages_2001-2010.csv", package="recordr"))
endocladia_coverage <- df[df$final_classification=="endocladia muricata",]
myDir <- tempdir()
csvOutFile <- sprintf("%s/Endocladia_muricata.csv", myDir)
write.csv(endocladia_coverage, file = csvOutFile)
plotFile <- sprintf("%s/emCoverage.png", myDir)
# If ggplot2 is available, create a plot
if(suppressWarnings(require(ggplot2))) {
  qplot(endocladia_coverage$longitude.dd., endocladia_coverage$latitude.dd., xlab="longitude", ylab="latitude",
         main="Endocladia muricata distribution")
  ggsave(plotFile, plot=last_plot())
}
