# 
# This file contains recordr functions that override the corresponding functions from R and DataONE.
# recordr overrides these functions so that provenance information can be recorded for the
# operations that these fuctions perform.
# calls the corresponding function in R or DataONE. For example, when the user's script calls
# D1get, the rD1get call is called here, provenance tasks are performed, then the real D1get is
# called.
# See the 'record' method to see how the overriding of the methods is performed.
#
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
  
  cat(sprintf("recordr_source: Sourcing file: %s", file))
  
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
  cat(sprintf("recordr_getD1Object: getting identifier: %s", identifier))
  d1o <- dataone::getD1Object(x, identifier)
  
  # Record the provenance relationship between the downloaded D1 object and the executing script
  # as 'script <- used <- D1Object
  # i.e. insertRelationship
  
  
  return(d1o)
})

# Override the rdataone 'getD1Object' method
# record the provenance relationship of script <- used <- D1Object
#
#' @export
setGeneric("recordr_createD1Object", function(x, d1Object, ...) { 
  standardGeneric("recordr_createD1Object")
})

setMethod("recordr_createD1Object", signature("D1Client", "D1Object"), function(x, d1Object) {
  
  cat(sprintf("recordr_createD1Object"))
  
  d1o <- dataone::getD1Object(x, identifier)
  
  # Record the provenance relationship between the downloaded D1 object and the executing script
  # as 'script <- used <- D1Object
  # i.e. insertRelationship
  
  return(d1o)
  
})
