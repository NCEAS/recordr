## Test environments

* OS X 10.11.6, R 3.3.1
* Ubuntu 14.04, R 3.3.1
* Windows 7, R 3.3.1 (i386, x86_64)
* Windows (via win-builder): x86_64-w64-mingw32 (64-bit) 3.3.1 (2016-06-21)
* Windows (via win-builder): x86_64-w64-mingw32 (64-bit) R Under development (unstable) (2016-08-03 r71023)

## R CMD check results

* There were no ERRORs or WARNINGs.
* There were 3 NOTEs:
  - This is a new submission.
  - Possibly mis-spelled words in DESCRIPTION:
    - DataONE (13:68)
      - A URL has been provided in the DESCRIPTION that describes the
        name "DataONE".
  - A NOTE regarding a call to attach in 'recordr/R/Record.R'
    This call is needed by the software so that a small set of
    "instrumented" functions can be called by temporarily altering
    the search path. Please note that this call is paired with
    an on.exit(recordrShutdown()) call (which calls detach) thereby
    ensuring that the search path will be restored when the function exits, as
    the R help for 'attach' describes in the section "Good practice".

## Downstream dependencies

* New submission, so no downstream dependencies currently exist.
