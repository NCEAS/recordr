#
#   This work was created by the National Center for Ecological Analysis and Synthesis.
#
#     Copyright 2015 Regents of the University of California
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
#' Record, review and publish data provenance.
#' @name recordr
#' @docType package
#' @aliases recordr
#' @description The R package \emph{recordr} provides methods to easily record data provenance about
#' R script executions, such as the files that were read and written by the script, along with information 
#' about the execution, such as start time end time, the R modules loaded during the execution, etc.
#' This provenance information along with any files created by the script can then be
#' combined into a data package and uploaded to a data repository such as DataONE.
#' @details
#' An overview of the recordr package is available with the R command: \code{'vignette("recordr_overview")'}.
#' @examples
#' \dontrun{
#' # This example shows how to record provenance for an R script and view the recorded 
#' information.
#' library(recordr)
#' rc <- new("Recordr")
#' record(rc, "./myScript.R", tag="Simple script recording #1")
#' listRuns(rc, tag="recording #1")
#' viewRuns(rc, tag="recording #1")
#' }
#' @author Peter Slaughter (NCEAS), Matthew B. Jones (NCEAS), Christopher Jones (NCEAS)
#' @section Classes:
#' \itemize{
#'  \item{\code{\link[=Recordr-class]{Recordr}}}{: A class containing methods to record, review and publish data provenance}
#' }
NULL
