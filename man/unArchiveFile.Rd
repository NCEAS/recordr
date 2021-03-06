% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Recordr.R
\name{unArchiveFile}
\alias{unArchiveFile}
\title{Remove a file from the recordr archive directory}
\usage{
unArchiveFile(recordr, fileId)
}
\arguments{
\item{recordr}{A Recordr object}

\item{fileId}{The fileId to remove from the archive}
}
\value{
A logical value - TRUE if the file is remove, FALSE if not
}
\description{
Remove a file from the recordr archive directory
}
\note{
This function is intended to run only during a record() session, i.e. the
recordr environment needs to be available.
}
