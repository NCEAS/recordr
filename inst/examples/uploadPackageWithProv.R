library(dataone)
library(datapack)
library(xml2)
library(digest)
library(uuid)
# Create a csv file for the science object
d1c <- D1Client("STAGING2", "urn:node:mnTestKNB")
# Set 'subject' to authentication subject, if available, so we will have permission to change this object
dp <- new("DataPackage")

# Create metadata object that describes science data
emlFile <- system.file("extdata/strix-pacific-northwest.xml", package="dataone")
metadataObj <- new("DataObject", format="eml://ecoinformatics.org/eml-2.1.1", filename=emlFile)
metadataId <- getIdentifier(metadataObj)
# Associate the metadata ttps://quality.nceas.ucsb.edu/quality/suites/arctic.data.center.suite.1/object 
# with each data object using the 'insertRelationships' method.
metadataObj <- addAccessRule(metadataObj, "http://orcid.org/0000-0003-2192-431X", "changePermission")
dp <- addMember(dp, metadataObj)
metadataId <- selectMember(dp, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")

sourceData <- system.file("extdata/OwlNightj.csv", package="dataone")
sourceObj <- new("DataObject", format="text/csv", filename=sourceData)
dp <- addMember(dp, sourceObj, metadataId)

resolveURL <- sprintf("%s/%s/object", d1c@mn@baseURL, d1c@mn@APIversion)
# Update the distribution URL in the metadata with the identifier that has been assigned to
# this DataObject. This provides a direct link between the detailed information for this package
# member and DataONE, which will assist DataONE in accessing and displaying this detailed information.
xpathToURL <- "//dataTable/physical/distribution[../objectName/text()=\"OwlNightj.csv\"]/online/url"
newURL <- sprintf("%s/%s", resolveURL, getIdentifier(sourceObj))
dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)

# When the metadata object is updated inside the datapackge, it's identifier may be
# updated, which will be used if the package is uploaded to the MN again, as the 'update' 
# (obsoleting) id.
metadataId <- selectMember(dp, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")

progFile <- system.file("extdata/filterObs.R", package="dataone")
progObj <- new("DataObject", format="application/R", filename=progFile, mediaType="text/x-rsrc")
dp <- addMember(dp, progObj, metadataId)

xpathToURL <- "//otherEntity/physical/distribution[../objectName/text()=\"filterObs.R\"]/online/url"
newURL <- sprintf("%s/%s", resolveURL, getIdentifier(progObj))
dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)
metadataId <- selectMember(dp, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")

outputData <- system.file("extdata/Strix-occidentalis-obs.csv", package="dataone")
outputObj <- new("DataObject", format="text/csv", filename=outputData )
outputObj <- addAccessRule(outputObj, "http://orcid.org/0000-0003-2192-431X", "changePermission")
dp <- addMember(dp, outputObj, metadataId)

xpathToURL <- "//dataTable/physical/distribution[../objectName/text()=\"Strix-occidentalis-obs.csv\"]/online/url"
newURL <- sprintf("%s/%s", resolveURL, getIdentifier(outputObj))
dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)

# It is also possible to add access rules to all (or a set) of the members in a package
dp <- addAccessRule(dp, "http://orcid.org/0000-0003-1234-5678", "read", getIdentifiers(dp))

# View a DataObject from the package, to see the access policy
getMember(dp, selectMember(pkg, name="sysmeta@fileName", value=basename(sourceData)))

dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj)

# Upload the data package to DataONE
resmapId <- uploadDataPackage(d1c, dp, public=TRUE, quiet=FALSE)

# Alternatively, the modified datapackage can also be returned from this call, in case
# further edits are required.
# newPkg <- uploadDataPackage(d1c, dp, public=TRUE, quiet=FALSE, as="DataPackage")
