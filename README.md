[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/recordr)](http://cran.r-project.org/web/packages/recordr)

- **Author**: Peter Slaughter, Matthew B. Jones, Christopher Jones ([NCEAS](http://www.nceas.ucsb.edu))
- **License**: [Apache 2](http://opensource.org/licenses/Apache-2.0)
- [Package source code on Github](https://github.com/NCEAS/recordr)
- [**Submit Bugs and feature requests**](https://github.com/NCEAS/recordr/issues)

The *recordr* R package provides an automated way to capture data provenance for R scripts and
console commands without the need to modify existing R code. The provenance that is captured during
an R script execution includes information about the script that was run, files that were read and
written, and details about the execution environment at the time of execution. R code, Input files and generated files from every script execution, or "run", can be efficiently archived so that these past versions of files can be retrieved for a run in order to investigate previous versions of processing or
analysis, support reproducibilty, and provide an easy way to publish data products and all files that
contributed to those products to a data repository such as the DataONE network.

A *recordr* overview vignette can be viewed at https://github.com/NCEAS/recordr/blob/master/vignettes/intro_recordr.Rmd.

## Installation Notes 

One of the R packages that recordr imports (the redland R package) depends on the Redland RDF libraries that must be
installed before installing *recordr*.

On Mac OSX you can use Macports to install the necessary libraries. From a terminal window
you can enter the command:

```
sudo port install redland
```

On Ubuntu the redland C libraries are installed from a terminal window with the commands:

```
sudo apt-get update
sudo apt-get install librdf0
sudo apt-get install librdf0-dev
sudo apt-get install pkg-config
```

Once the Redland RDF libraries are installed, the *recordr* package can be installed.
Please note that the *recordr* package is not yet available via CRAN but a pre-release version of *recordr* and
the R packages it depends on can be installed via the NCEAS drat repository. The *recordr* package also
depends on the rOpenSci *EML* package that is also available as a pre-release R package.

From the R console, enter the following commands:

```r
library(devtools)
install_github("ropensci/EML", build=FALSE, dependencies=c("DEPENDS", "IMPORTS"))
install.packages(drat)
library(drat)
addRepo("NCEAS")
install.packages("recordr")
library(recordr)
```
  
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

[![nceas_footer](https://www.nceas.ucsb.edu/files/newLogo_0.png)](http://www.nceas.ucsb.edu)
