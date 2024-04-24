# Bias correction of ICAR CMIP data to the livneh grid

This document describes the Bias correction process of ICAR data to the Livneh grid.

### Prerequisites
 - Postprocess the raw (or 3hr raw) ICAR output first: dir `CMIP_ICAR_postprocess` (on github: `https://github.com/bertjebertjek/CMIP_ICAR_postprocess`)
    This does:
        - correction of negative precipitation due to restart errors
        - aggregation into monthly (yearly) files w 3hr (24hr) timestep
        - removes the GCM's convective precipitation (cp) from the ICAR precipitation
        - removes unwanted output variables (optional)


## 1. Regrid the post-processed ICAR data to the livneh grid:
    in subdir `1_Regrid`
    This is required before we can bias correct to the livneh grid.
    Set in- and output paths, models, scenarios, and timestep (daily or 3hr) in  `submit_regrid2Liv_all.sh`



## 2. The Bias Correction

Bias correction is split into separate scripts /procedures for precipitation and temperature, because the runtime would otherwise exceed 12 hrs: `BC_Icar2Liv_5y_PCP.py` and `BC_Icar2Liv_5y_ta2m.py`

These script use Quantile mapping (in separate functions/files) with historical CMIPX-ICAR simulation as reference data, and Livneh as observations. QM is done in 5 year blocks of input data, where this 5 year period is excluded from the reference data, in order to preserve future extremes.

These functions can be called separately for each model & scenario from submit_BC2liv_CMIP{5/6}_{pcp/ta2m}.sh, or for all models and scenarios from `loop_submit.sh`. This loop_submit script sets the correct model/scenario parameters by modifying a pbs template script (located in dir `auto_submit` ), and then launches 2 jobs per model/scenario combination; one for the precipitation bias correction, and a second job, dependent on the first, for the temperature bias correction.

Expect runtimes of 12+ hours for each model/scenario combination. Memory use is currently set at 150 GB per script.

### Further info: Precipitation BC
- There is a flag to add noise to the input and reference data during the bias correction procedure in `BC_Icar2Liv_5y_pcp.py`. Default=True


### Further info: Temperature BC
- relative humidity can be added as a variable to the output dataset calc_relhum. Default is True.
- ...



### Post processing
If there are still variables in the output we don;t need, use `remove_vars_and_float.py` to remove them and make all other variables float32 iso float64. If the preprocessing was done with the procedures in dir `../CMIP_ICAR_postprocess` this should not be necessary.



_Bert Kruyt , NCAR RAL, february 2024_

