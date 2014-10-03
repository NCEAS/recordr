library(dataone)

# This test script can be used to test the 'record' method of the recordr package.
# This script downloads a dataset from DataONE and creates a local CSV file from
# this dataset.
# The provenance relationships that should be recorded are:
#     testData.csv <= wasGeneratedBy <- sampleUserScript.R
#     sampleUserScript.R <- used <- doi:10.6085/AA/pisco_intertidal_summary.42.3
#
print("Sample User Script")
cm <- CertificateManager()
getCertExpires(cm)

cli <- D1Client("SANDBOX", "urn:node:mnSandboxUCSB1")

# Download a single D1 object
#item <- getD1Object(cli, "doi:10.6085/AA/ELLXXX_015MTBD003R00_20040828.40.1")
item <- getD1Object(cli, "doi:10.6085/AA/pisco_intertidal_summary.42.3")

# Pull out data as a data frame
df <- asDataFrame(item)

# Save local file
write.csv(df, file = "testData.csv")