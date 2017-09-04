[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/recordr)](https://cran.r-project.org/package=recordr)

- **Author**: Peter Slaughter, Matthew B. Jones, Christopher Jones, Lauren Palmer ([NCEAS](http://www.nceas.ucsb.edu))
- [doi:10.5063/F1GF0RF6](http://doi.org/10.5063/F1GF0RF6)
- **License**: [Apache 2](http://opensource.org/licenses/Apache-2.0)
- [Package source code on Github](https://github.com/NCEAS/recordr)
- [**Submit Bugs and feature requests**](https://github.com/NCEAS/recordr/issues)

The *recordr* R package provides an automated way to capture data provenance for R scripts and
console commands without the need to modify existing R code. The provenance that is captured during
an R script execution includes information about the script that was run, files that were read and
written, and details about the execution environment at the time of execution. R code, Input files and generated files from every script execution, or "run", can be efficiently archived so that these past versions of files can be retrieved for a run in order to investigate previous versions of processing or
analysis, support reproducibility, and provide an easy way to publish data products and all files that
contributed to those products to a data repository such as the DataONE network.

A *recordr* overview vignette can be viewed at https://github.com/NCEAS/recordr/blob/master/vignettes/intro_recordr.Rmd.

## Installation Notes 

The *recordr* R package requires the R package *redland*. If you are installing on Ubuntu then the Redland C libraries
must be installed first. If you are installing on Mac OS X or Windows then installing these libraries is not required.

### Installing on Mac OS X

On Mac OS X *recordr* can be installed with the following commands:

```
install.packages("recordr")
library(recordr)
```

The *recordr* R package should be available for use at this point.

Note: if you wish to build the required *redland* package from source before installing *recordr*, please see the redland [installation instructions]( https://github.com/ropensci/redland-bindings/tree/master/R/redland).

### Installing on Ubuntu

For ubuntu, install the required Redland C libraries by entering the following commands
in a terminal window:

```
sudo apt-get update
sudo apt-get install librdf0 librdf0-dev
```

Then install the R package from the R console:

```
install.packages("recordr")
library(recordr)
```

The *recordr* R package should be available for use at this point

### Installing on Windows

For windows, the required *redland* R package is distributed as a binary release, so it is not
necessary to install any additional system libraries.

To install the *recordr* R packages from the R console:

```
install.packages("recordr")
library(recordr)
```
  
The *recordr* R package should be available for use at this point.

Note: if you wish to build the required *redland* package from source before installing *recordr*, please see the redland [installation instructions]( https://github.com/ropensci/redland-bindings/tree/master/R/redland).

## License

The `recordr` package is licensed as open source software under the Apache 2.0 license.

## Example Usage

The `recordr` package can be used to track code execution in R, data inputs and outputs to 
those executions, and the software environment during the execution (e.g., R, OS versions).  
Some examples are provided in the overview vignette.  As a quick start, here is an example that
starts recordr, executes a precanned R script, and then views the details of that script run.

```r
library(recordr)
rc <- new("Recordr")
record(rc, system.file("extdata/EmCoverage.R", package="recordr"), tag="First recordr run")
listRuns(rc)
viewRuns(rc)
```

## Acknowledgements
Work on this package was supported by NSF-ABI grant #1262458 to C. Gries, M. Jones, and S. Collins. Additional support
was provided for working group collaboration by the National Center for Ecological Analysis and Synthesis, a Center funded by the University of California, Santa Barbara, and the State of California.

[![nceas_footer](https://www.nceas.ucsb.edu/files/newLogo_0.png)](http://www.nceas.ucsb.edu)
