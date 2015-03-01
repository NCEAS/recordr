# 
# This file contains recordr functions that override the corresponding functions from R and DataONE.
# recordr overrides these functions so that provenance information can be recorded for the
# operations that these fuctions perform.
# calls the corresponding function in R or DataONE. For example, when the user's script calls
# D1get, the rD1get call is called here, provenance tasks are performed, then the real D1get is
# called.
# See the 'record' method to see how the overriding of the methods is performed.
#
#' @import dataone
#' @include Constants.R

#' Override the DataONE MNode::get function so that recordr can record when the user's script uses a DataONE dataset
#' @export
setGeneric("recordr_D1MNodeGet", function(node, pid) {
  standardGeneric("recordr_D1MNodeGet")
})

setMethod("recordr_D1MNodeGet", signature("MNode", "character"), function(node, pid) {
  
  # Call the masked function to retrieve the DataONE object
  #cat(sprintf("In recordr_D1MNodeGet\n"))
  d1o <- dataone::get(node, pid)
  
  # Write provenance info for this object to the DataPackage object.
  if (getProvCapture()) {
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    
    # Record the DataONE resolve service endpoint + pid for the object of the RDF triple
    D1_resolve_pid <- sprintf("%s/%s", D1_CN_Resolve_URL, pid)    
    # Record prov:used relationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=D1_resolve_pid, predicate=provUsed)
    setProvCapture(TRUE)
  }
  return(d1o)
  
})
    
# Override the 'source' function so that recordr can detect when the user's script sources another script
## @export
# setGeneric("recordr_source", function(file, ...) {
#   standardGeneric("recordr_source")
# })
# 
# setMethod("recordr_source", "character", function (file, local = FALSE, echo = verbose, print.eval = echo,
#                                                    verbose = getOption("verbose"), prompt.echo = getOption("prompt"),
#                                                    max.deparse.length = 150, chdir = FALSE, encoding = getOption("encoding"),
#                                                    continue.echo = getOption("continue"), skip.echo = 0,
#                                                    keep.source = getOption("keep.source")) {
#   if(length(verbose) == 0)
#     verbose = FALSE
#   
#   if(chdir) {
#     cwd = getwd()
#     on.exit(setwd(cwd))
#     setwd(dirname(file))
#   }
#   
#   #cat(sprintf("recordr_source: Sourcing file: %s\n", file))
#   
#   base::source(file, local, echo, print.eval, verbose, prompt.echo,
#                max.deparse.length, chdir, encoding,continue.echo, skip.echo,
#                keep.source)
#   
#   # Record the provenance relationship between the sourcing script and the sourced script
#   # as 'sourced script <- wasInflucedBy <- sourcing script
#   # i.e. insertRelationship
# 
# })

# Override the DataONE 'MNODE:create' method
#setMethod("recordr_create", signature("MNode", "character"), function(mnode, pid, filepath, sysmeta) {
#  print("in method recordr_create")
#}

# Override the rdataone 'getD1Object' method
# record the provenance relationship of script <- used <- D1Object
#
## @export
# setGeneric("recordr_getD1Object", function(x, identifier, ...) { 
#   standardGeneric("recordr_getD1Object")
# })
# 
# setMethod("recordr_getD1Object", "D1Client", function(x, identifier) {
#   d1o <- dataone::getD1Object(x, identifier)
#   
#   # Record the provenance relationship between the downloaded D1 object and the executing script
#   # as 'script <- used <- D1Object
#   # i.e. insertRelationship
#   # Record the provenance relationship between the user's script and the derived data file
#   if (getProvCapture()) {
#     #cat(sprintf("recordr_getD1Obj: recording prov for: %s\n", identifier))
#     scriptPath <- get("scriptPath", envir = as.environment(".recordr"))
#     ##d1Client <- get("d1Client", envir = as.environment(".recordr"))
#     ##dataPkg <- get("dataPkg", envir = as.environment(".recordr"))
#     outLines <- sprintf("%s used %s", basename(scriptPath), identifier)
#     runDir <- get("runDir", envir = as.environment(".recordr"))
#     write(outLines, sprintf("%s/%s/prov.txt", runDir, execMeta@executionId), append = TRUE)
#   }
#   
#   return(d1o)
# })

# Override the rdataone 'createD1Object' method
# record the provenance relationship of script <- used <- D1Object
#
## @export
# setGeneric("recordr_createD1Object", function(x, d1Object, ...) { 
#   standardGeneric("recordr_createD1Object")
# })
# 
# setMethod("recordr_createD1Object", signature("D1Client", "D1Object"), function(x, d1Object, ...) {
#   
#   #cat(sprintf("recordr_createD1Object"))
#   d1o <- dataone::getD1Object(x, identifier)
#   
#   # Record the provenance relationship between the downloaded D1 object and the executing script
#   # as 'script <- used <- D1Object
#   # i.e. insertRelationship
#   
#   return(d1o)
#   
# })

# Register "textConnection" as an S4 class so that we use it in the
# method signatures below.
setOldClass("textConnection", "connection")

# Override the R 'write.csv' method
# record the provenance relationship of local objecct <- wasGeneratedBy <- script
#' @export
setGeneric("recordr_write.csv", function(x, file, ...) {
  standardGeneric("recordr_write.csv")
})

setMethod("recordr_write.csv", signature("data.frame", "character"), function(x, file, ...) {
  
  #cat(sprintf("In recordr_write.csv\n"))
  # Call the original function that we are overriding
  obj <- utils::write.csv(x, file, ...)
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture()) {
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    user <- recordrEnv$execMeta@accountName
    #datasetId <- sprintf("%s_%s.%s", tools::file_path_sans_ext(basename(file)), UUIDgenerate(), tools::file_ext(file))
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    con <- textConnection("data", "w", local=TRUE)
    utils::write.csv(x, file=con, ...)
    close(con)
    csvdata <- charToRaw(paste(data, collapse="\n"))
    # Create a data package object for the derived dataset
    dataFmt <- "text/csv"
    dataObj <- new("DataObject", datasetId, csvdata, dataFmt, user, recordrEnv$mnNodeId)
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addData(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provWasGeneratedBy)
    saveFileInfo(datasetId, file)
    setProvCapture(TRUE)
  }
  return(obj)
})

setMethod("recordr_write.csv", signature("data.frame", "textConnection"), function(x, file, ...) {
  #cat(sprintf("recordr_write.csv for textConnection\n"))
  obj <- utils::write.csv(x, file, ...)
})

#' Override the R 'read.csv' method 
#' @description record the provenance relationship of local objecct <- wasGeneratedBy <- script
#' @export
setGeneric("recordr_read.csv", function(...) { 
  standardGeneric("recordr_read.csv")
})

setMethod("recordr_read.csv", signature(), function(...) {
  #cat(sprintf("In recordr_read.csv\n"))
  dataRead <- utils::read.csv(...)
  # Record the provenance relationship between the user's script and an input data file.
  # If the user didn't specify a data file, i.e. they are reading from a text connection,
  # then exit, as we don't track provenance for text connections. With read.csv, a
  # text connection can be specified by omitting the 'file' argument and specifying the
  # 'text' argument.
  argList <- list(...)
  argListLen <- length(argList)
  if (!"file" %in% names(argList) && "text" %in% names(argList)) {
    #cat(sprintf("text connection: %s", argList$text))
    return(dataRead)
  } else if ("file" %in% names(argList)) {
    #cat(sprintf("file: %s\n", argList$file))
    fileArg <- argList$file
  } else if (!"file" %in% names(argList) && !"text" %in% names(argList)) {
    #cat(sprintf("file: %s\n", argList[1]))
    fileArg <- argList[1]
  } else {
    cat(paste0("Error: unknown arguments passed to record_read.csv: ", argList))
  }
  
  if (getProvCapture()) {
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    # TODO: replace this with a user configurable faciltiy to specify how to generate identifiers
    #datasetId <- sprintf("%s_%s", basename(fileArg), UUIDgenerate())
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Record prov:wasUsedBy relationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    saveFileInfo(datasetId, fileArg)
    
    setProvCapture(TRUE)
  }
  return(dataRead)
})

setMethod("recordr_read.csv", signature("textConnection"), function(file, ...) {
  print("recordr_read.csv for textConnection\n")
  obj <- utils::read.csv(file, ...)
})

#' Disable or enable provenance capture temporarily
#' It may be necessary to disable provenance capture temporarily, for example when
#' record() is writting out a housekeeping file.
#' A state variable in the ".recordr" environment is used to
#' temporarily disable provenance capture so that housekeeping tasks
#' will not have provenance information recorded for them.
#' Return the state of provenance capture: TRUE is enalbed, FALSE is disabled
#' @param enable logical variable used to enable or disable provenance capture
#' @return enabled a logical indicating the state of provenance capture: TRUE=enabled, FALSE=disabled
#' @author slaughter
#' @export
setGeneric("setProvCapture", function(enable) {
  standardGeneric("setProvCapture")
})

setMethod("setProvCapture", signature("logical"), function(enable) {
  # If the '.recordr' environment hasn't been created, then we are calling this
  # function outside the context of record(), so don't attempt to update the environment'
  if (is.element(".recordr", base::search())) {
    assign("provCaptureEnabled", enable, envir = as.environment(".recordr"))    
    return(enable)
  } else {
    # If we were able to update "provCaptureEnabled" state variable because env ".recordr"
    # didn't exist, then provenance capture is certainly not enabled.    
    return(FALSE)
  }
})

#' Return current state of provenance capture
#' @return enabled a logical indicating the state of provenance capture: TRUE=enabled, FALSE=disabled
#' @export
setGeneric("getProvCapture", function(x) {
  standardGeneric("getProvCapture")
})

setMethod("getProvCapture", signature(), function(x) {
  # The default state for provenance capture is enabled = FALSE. Currently in this package,
  # provenance capture is only enabled when the record() function is running.
  #
  # If the '.recordr' environment hasn't been created, then we are calling this
  # function outside the context of record(), so don't attempt to read from the environment.
  if (is.element(".recordr", base::search())) {
    if (exists("provCaptureEnabled", where = ".recordr", inherits = FALSE )) {
      enabled <- base::get("provCaptureEnabled", envir = as.environment(".recordr"))
    } else {
      enabled <- FALSE
    }
  } else {
    enabled <- FALSE
  }
  return(enabled)
})

setMethod("publish", signature("Recordr", "character", "MNode"), function(recordr, packageId, MNode) {
  print(paste("publishing package: ", packageId))
})

# Save local file information
saveFileInfo <- function(dataObjId, file) {
  
  # Disable provenance capture while we read/write file info
  provEnabled <- getProvCapture()
  setProvCapture(FALSE)
  
  # Construct directory/filename to store file info
  recordrEnv <- as.environment(".recordr")
  infoFile <- sprintf("%s/runs/%s/fileInfo.csv", recordrEnv$recordrDir, recordrEnv$execMeta@executionId)
  filePath <- normalizePath(file)
  # get info for this file. file.info stores dates as POSIXct, so convert them to strings
  # so that they don't get written out as an integer timestamp, i.e. milliseconds since ref date
  rawInfo <- base::file.info(filePath)
  thisFstats <- data.frame(dataObjId=dataObjId, size=rawInfo[["size"]], 
                           mtime=as.character(rawInfo[["mtime"]]), 
                           ctime=as.character(rawInfo[["ctime"]]), 
                           uname=rawInfo[["uname"]],
                           stringsAsFactors=FALSE, row.names=NULL)
  rownames(thisFstats) <- c(filePath)
  # Read in the stored file info for this execution. This file contains info for all
  # files used by this execution. Rowname of data frame is the file path.
  if (file.exists(infoFile)) {
    fstats <- read.csv(infoFile, stringsAsFactors=FALSE, row.names = 1)
      # Replace or add the entry for this file
      fstats[filePath, ] <- thisFstats
  } else {
    # First file recorded, just write one file info out
    fstats <- thisFstats
  }
  # Save file info to run directory
  write.csv(fstats, infoFile, row.names = TRUE)
  setProvCapture(provEnabled)
}

# Retrieved all previously stored information for local files 
getFileInfo <- function(recordr, id) {
  # Disable provenance capture while we read/write file info
  provEnabled <- getProvCapture()
  setProvCapture(FALSE)
  thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  infoFile <- sprintf("%s/fileInfo.csv", thisRunDir)
  fstats <- read.csv(infoFile, stringsAsFactors=FALSE, row.names = 1)
  if (! file.exists(infoFile)) {
    msg <- sprintf("Information file %s not found for execution identifier: %s", infoFile, id)
    stop(msg)
  }
  fstats <- read.csv(infoFile, stringsAsFactors=FALSE, row.names = 1)
  setProvCapture(provEnabled)
  return(fstats)
}

archiveFile <- function(file) {
}
