## Test environments

* OS X 10.11.6, R 3.3.1
* Ubuntu 14.04, R 3.3.1
* Windows 7, R 3.3.1 (i386, x86_64)
* Windows (via win-builder): x86_64-w64-mingw32 (64-bit) 3.3.1 (2016-06-21)
* Windows (via win-builder): x86_64-w64-mingw32 (64-bit) R Under development (unstable) (2016-08-28 r71162)

## R CMD check results

* There were no ERRORs or WARNINGs.
* There were 3 NOTEs:
  - This is a new submission.
  - Possibly mis-spelled words in DESCRIPTION:
    - DataONE (13:68)
      - A URL has been provided in the DESCRIPTION that describes the
        name "DataONE".
  - A NOTE regarding a call to attach in 'recordr/R/Record.R'
    - It is critical to the operation of the recordr package that it temporarily attaches the 
      environment ".recordr" to the search path to hold the function bindings that will 
      temporarily override a select set of functions with the recordr package version, for example:

        attach(NULL, name=".recordr")
        recordrEnv <- as.environment(".recordr")
        recordrEnv$ggsave <- recordr::recordr_ggsave
        
      Overriding functions in this manner ensures that the recordr version of a function will 
      be called regardless of package load order, provided that the function is not called with a 
      fully qualified name, i.e. `ggplot2::ggsave', or if the function is inside a package that
      imports the function. Not all instances of a function call will be recorded, but certainly
      the function calls that recordr overrides and are called from a user scripts will be
      recorded.
      
      When one of our overridding functions is called, we record the 
      data provenance for the function, i.e. the files specified for read and/or write, then 
      call the intended function, i.e. ggplot2::ggsave.

      Using this mechanism, we only override functions while record() is running. 
    
      Please note that the first call made in the `record()` function is `on.exit(recordrShutdown)`, 
      which simply calls `detach(".recordr")`, thereby ensuring that the search path will be restored
      when  `record()` exits.

## Downstream dependencies

* New submission, so no downstream dependencies currently exist.
