#
#   This work was created by participants in the DataONE project, and is
#   jointly copyrighted by participating institutions in DataONE. For
#   more information on DataONE, see our web site at http://dataone.org.
#
#     Copyright 2014
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

## A class representing a script execution run manager
#' @include Recordr.R
#' @slot name (not currently used)
#' @author slaughter
#' @export
setClass("ExecMetadata", slots = c(executionId      = "character",
                                   tag              = "character",
                                   datapackageId    = "character",
                                   accountName      = "character",
                                   hostId           = "character",
                                   startTime        = "character",
                                   operatingSystem  = "character",
                                   runtime          = "character",
                                   softwareApplication = "character",
                                   moduleDependencies  = "character",
                                   endTime             = "character",
                                   errorMessage        = "character",
                                   publishTime         = "character"))

############################
## ExecMetadata constructors
############################

#' execution metadata
#' @param ... (not yet used)
#' @return the ExecMetadata object
#' @author slaughter
#' @export
setGeneric("ExecMetadata", function(programName, tag="") {
  standardGeneric("ExecMetadata")
})

setMethod("ExecMetadata", signature("character"), function(programName, tag="") {
  
  ## create new MNode object and insert uri endpoint
  execMeta <- new("ExecMetadata")  
  execMeta@executionId <- UUIDgenerate()
  execMeta@tag         <- tag
  execMeta@datapackageId <- UUIDgenerate()
  execMeta@accountName <- Sys.info()[['user']]
  execMeta@hostId <- Sys.info()[['nodename']]
  execMeta@startTime <- as.character(Sys.time())
  execMeta@operatingSystem <- R.Version()$platform
  execMeta@runtime <- R.Version()$version.string
  execMeta@softwareApplication  <- programName
  execMeta@endTime <- ""
  execMeta@errorMessage <- ""
  execMeta@publishTime <- ""
  # Get list of packages that recordr has loaded and store as characters, i.e.
  # "recordr 0.1, uuid 0.1-1, dataone 1.0.0, dataonelibs 1.0.0, XML 3.98-1.1, rJava 0.9-6"
  pkgs <- sessionInfo()$otherPkgs
  execMeta@moduleDependencies <- paste(lapply(pkgs, function(x) paste(x$Package, x$Version)), collapse = ', ')
  return(execMeta)
})

##########################
## Methods
##########################

#' Write execution metadata to disk
#' 
#' @param ExecMetadata object
#' @param The filename to serialize the execution metadata to
#' @author slaughter
#' @export
setGeneric("writeExecMeta", function(recordr, execMeta, ...) {
  standardGeneric("writeExecMeta")
})

setMethod("writeExecMeta", signature("Recordr", "ExecMetadata"), function(recordr, execMeta, ...) {
  #print(sprintf("writeExecMeta: writing file %s/runs/%s/execMetadata.csv", recordr@recordrDir, execMeta@executionId))
  
  # Get values from all the slots for the execution metadata, in order.
  # There is probably an easier way to do this!
  slotNames <- names(getSlots("ExecMetadata"))
  slotValues <- as.character(lapply(slotNames, function(x) eval(slot(execMeta, x))))
  df <- data.frame(name = slotNames, value = slotValues)
  provCaptureEnabled <- getProvCapture()
  outDir <- sprintf("%s/runs/%s", recordr@recordrDir, execMeta@executionId)
  
  if (! file.exists(outDir)) {
    dir.create(outDir, recursive = TRUE)
  }
  # Disable provenance capture while we write out this housekeeping file
  setProvCapture(FALSE)
  outFilePath <- sprintf("%s/%s", outDir, "execMetadata.csv")
  write.csv(df, outFilePath, row.names = TRUE)
  setProvCapture(provCaptureEnabled)
  return(outFilePath)
})

#' Read Execution metadata from disk
#' @param identifier the run identifier to read execution metadata for
## @return an execution metadata object
## 
## @author slaughter
#' @export
setGeneric("readExecMeta", function(recordr, executionId) {
  standardGeneric("readExecMeta")
})

setMethod("readExecMeta", signature("Recordr", "character"), function(recordr, executionId) {
  filePath <- sprintf("%s/runs/%s/%s", recordr@recordrDir, executionId, "execMetadata.csv")
  if (file.exists(filePath)) {
    # Temporarily disable provenance capture while we read in execution metadata
    provCaptureEnabled <- getProvCapture()
    setProvCapture(FALSE)
    mdf <- read.csv(filePath, stringsAsFactors=FALSE)
    setProvCapture(provCaptureEnabled)
    execMeta <- new("ExecMetadata")  
    execMeta@executionId         <- mdf[mdf$name == "executionId", "value" ]
    execMeta@tag                 <- mdf[mdf$name == "tag", "value"]
    execMeta@datapackageId       <- mdf[mdf$name == "datapackageId", "value"]
    execMeta@accountName         <- mdf[mdf$name == "accountName", "value"]
    execMeta@hostId              <- mdf[mdf$name == "hostId", "value"]
    execMeta@startTime           <- mdf[mdf$name == "startTime", "value"]
    execMeta@operatingSystem     <- mdf[mdf$name == "operatingSystem", "value"]
    execMeta@runtime             <- mdf[mdf$name == "runtime", "value"]
    execMeta@softwareApplication <- mdf[mdf$name == "softwareApplication", "value"]
    execMeta@endTime             <- mdf[mdf$name == "endTime", "value"]
    execMeta@errorMessage        <- mdf[mdf$name == "errorMessage", "value"]
    execMeta@publishTime         <- mdf[mdf$name == "publishTime", "value"]
    execMeta@moduleDependencies  <- mdf[mdf$name == "moduleDependencies", "value"]
  } else {
    return(NULL)
  }
  return(execMeta)
})
