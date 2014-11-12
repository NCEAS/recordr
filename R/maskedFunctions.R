# 
# This file contains recordr functions that override the corresponding functions from R and DataONE.
# recordr overrides these functions so that provenance information can be recorded for the
# operations that these fuctions perform.
# calls the corresponding function in R or DataONE. For example, when the user's script calls
# D1get, the rD1get call is called here, provenance tasks are performed, then the real D1get is
# called.
# See the 'record' method to see how the overriding of the methods is performed.
#
library(dataone)
## @include Recordr.R

# Override the 'source' function so that recordr can detect when the user's script sources another script
#' @export
setGeneric("recordr_source", function(file, ...) {
  standardGeneric("recordr_source")
})

setMethod("recordr_source", "character", function (file, local = FALSE, echo = verbose, print.eval = echo,
                                                   verbose = getOption("verbose"), prompt.echo = getOption("prompt"),
                                                   max.deparse.length = 150, chdir = FALSE, encoding = getOption("encoding"),
                                                   continue.echo = getOption("continue"), skip.echo = 0,
                                                   keep.source = getOption("keep.source")) {
  if(length(verbose) == 0)
    verbose = FALSE
  
  if(chdir) {
    cwd = getwd()
    on.exit(setwd(cwd))
    setwd(dirname(file))
  }
  
  #cat(sprintf("recordr_source: Sourcing file: %s\n", file))
  
  base::source(file, local, echo, print.eval, verbose, prompt.echo,
               max.deparse.length, chdir, encoding,continue.echo, skip.echo,
               keep.source)
  
  # Record the provenance relationship between the sourcing script and the sourced script
  # as 'sourced script <- wasInflucedBy <- sourcing script
  # i.e. insertRelationship

})

# Override the DataONE 'MNODE:create' method
#setMethod("recordr_create", signature("MNode", "character"), function(mnode, pid, filepath, sysmeta) {
#  print("in method recordr_create")
#}

# Override the rdataone 'getD1Object' method
# record the provenance relationship of script <- used <- D1Object
#
#' @export
setGeneric("recordr_getD1Object", function(x, identifier, ...) { 
  standardGeneric("recordr_getD1Object")
})

setMethod("recordr_getD1Object", "D1Client", function(x, identifier) {
  d1o <- dataone::getD1Object(x, identifier)
  
  # Record the provenance relationship between the downloaded D1 object and the executing script
  # as 'script <- used <- D1Object
  # i.e. insertRelationship
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture()) {
    #cat(sprintf("recordr_getD1Obj: recording prov for: %s\n", identifier))
    scriptPath <- get("scriptPath", envir = as.environment(".recordr"))
    ##d1Client <- get("d1Client", envir = as.environment(".recordr"))
    ##d1Pkg <- get("d1Pkg", envir = as.environment(".recordr"))
    outLines <- sprintf("%s used %s", basename(scriptPath), identifier)
    runDir <- get("runDir", envir = as.environment(".recordr"))
    write(outLines, sprintf("%s/%s/prov.txt", runDir, execMeta@executionId), append = TRUE) 
  }
  
  return(d1o)
})

# Override the rdataone 'getD1Object' method
# record the provenance relationship of script <- used <- D1Object
#
#' @export
setGeneric("recordr_createD1Object", function(x, d1Object, ...) { 
  standardGeneric("recordr_createD1Object")
})

setMethod("recordr_createD1Object", signature("D1Client", "D1Object"), function(x, d1Object, ...) {
  
  #cat(sprintf("recordr_createD1Object"))
  d1o <- dataone::getD1Object(x, identifier)
  
  # Record the provenance relationship between the downloaded D1 object and the executing script
  # as 'script <- used <- D1Object
  # i.e. insertRelationship
  
  return(d1o)
  
})

# Register "textConnection" as an S4 class so that we use it in the
# method signatures below.
setOldClass("textConnection", "connection")

# Override the R 'write.csv' method
# record the provenance relationship of local objecct <- wasGeneratedBy <- script
#
#' @export
setGeneric("recordr_write.csv", function(x, file, ...) {
  standardGeneric("recordr_write.csv")
})

setMethod("recordr_write.csv", signature("data.frame", "character"), function(x, file, ...) {
  
  # Call the original function that we are overriding
  obj <- utils::write.csv(x, file, ...)
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture()) {
    #cat(sprintf("recordr_write.csv: recording prov for %s\n", file))
    scriptPath <- get("scriptPath", envir = as.environment(".recordr"))
    d1Client <- get("d1Client", envir = as.environment(".recordr"))
    d1Pkg <- get("d1Pkg", envir = as.environment(".recordr"))
    setProvCapture(FALSE)
    derived.data <- convert.csv(d1Client, x)
    setProvCapture(TRUE)
    #derivedDataId <- file
    programId <- scriptPath
    outLines <- sprintf("%s wasGeneratedBy %s", basename(file), basename(scriptPath))
    #execMeta <- get("execMeta", envir = as.environment(".recordr"))
    runDir <- get("runDir", envir = as.environment(".recordr"))
    write(outLines, sprintf("%s/%s/prov.txt", runDir, execMeta@executionId), append = TRUE)
    
    #d1Object.result <- new(Class="D1Object", id.derived, derived.data, format.result, d1Client@mn.nodeid)
    #addData(d1Pkg, d1Object.result)
    #insertRelationship(d1Pkg, id.result, c(programId), "http://www.w3.org/ns/prov", "http://www.w3.org/ns/prov#wasGeneratedBy")
    
    # Replace the data package in the ".recordr" environment
    #rm("d1Pkg", envir = as.environment(".test"))
    #assign("d1Pkg", d1Pkg, envir = as.environment(".recordr"))
  }
  return(obj)
})

setMethod("recordr_write.csv", signature("data.frame", "textConnection"), function(x, file, ...) {
  #cat(sprintf("recordr_write.csv for textConnection\n"))
  obj <- utils::write.csv(x, file, ...)
})
# Override the R 'read.csv' method
# record the provenance relationship of local objecct <- wasGeneratedBy <- script
#
#' @export
setGeneric("recordr_read.csv", function(file, ...) { 
  standardGeneric("recordr_read.csv")
})

setMethod("recordr_read.csv", signature("character"), function(file, ...) {
  df <- utils::read.csv(file, ...)
  # Record the provenance relationship between the user's script and the derived data file
  
  if (getProvCapture()) {
    #cat(sprintf("recordr_read.csv: recording prov for %s\n", file))
    scriptPath <- get("scriptPath", envir = as.environment(".recordr"))
    outLines <- sprintf("%s used %s", basename(scriptPath), basename(file))
    runDir <- get("runDir", envir = as.environment(".recordr"))
    write(outLines, sprintf("%s/%s/prov.txt", runDir, execMeta@executionId), append = TRUE)
  }
  return(df)
})

setMethod("recordr_read.csv", signature("textConnection"), function(file, ...) {
  print("recordr_read.csv for textConnection")
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
#' @author slaughter
#' @export
setGeneric("setProvCapture", function(enable) {
  standardGeneric("setProvCapture")
})

setMethod("setProvCapture", signature("logical"), function(enable) {
  # If the '.recordr' environment hasn't been created, then we are calling this
  # function outside the context of record(), so don't attempt to update the environment
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
