#!/usr/bin/env python
# coding: utf-8
############################################################################################
#
# Regrid ICAR to the Livneh grid
#  - make sure GCM cp is removed first(!)
#  - lakes masked and filled in for ta2m /Tmin /Tmax
#  - 2024-05-17 Relhum is added (calculated) before regridding, iso after bc.
#
#
# Authors:  Bert Kruyt, NCAR RAL 2023
############################################################################################

import argparse
import os
import time
import matplotlib.pyplot as plt
import xarray as xr
import xesmf as xe
import numpy as np
import glob, os
import dask
import sys
# sys.path.append('/glade/u/home/bkruyt/libraries/storylines/storylines')
sys.path.append('/glade/u/home/bkruyt/libraries')
sys.path.append('/glade/u/home/bkruyt/libraries/st_lines')
sys.path
import icar2gmet as i2g
# from storylines.tools import quantile_mapping # not used here but in bc_funcs.py
### If storylines gives an error , use py3_yifan kernel /conda env
# alternatively ../Example_scripts/2D_quantile_map/quantile_mapping.py



#--------------  SETTTINGS  ---------------
calc_relhum = True  # add relative humidity to dataset before regridding

# geo_em file for masking lakes:
geo_file='/glade/work/bkruyt/WPS/ICAR_domains/CMIP_WUS/geo_em.d01.nc'



############################################
#                functions
############################################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='Aggregate 1 day files to month(3h) and year (24h), while also fixing neg precip')
    parser.add_argument('year',   help='year to process')
    parser.add_argument('model',     help='model')
    parser.add_argument('scenario',  help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")
    parser.add_argument('path_in',   help='path 3h/daily files (should have these as subdirs)')
    parser.add_argument('path_out',  help='path to write to')
    parser.add_argument('CMIP',      help='CMIP5 or CMIP6')

    return parser.parse_args()

def crop_nan(ds, n):
    """Set outer n grid cells to NaN"""
    # Iterate over each variable in the dataset
    for var_name in ds.variables:
        if len(ds[var_name].dims)==3 and 'lat' in ds[var_name].dims and 'lon' in ds[var_name].dims:
            print(var_name)
            var = ds[var_name]
            # Set outer n grid cells to NaN
            var[:, :, :n] = np.nan  # Set leftmost columns to NaN
            var[:, :, -n:] = np.nan  # Set rightmost columns to NaN
            var[:, :n, :] = np.nan  # Set top rows to NaN
            var[:, -n:, :] = np.nan  # Set bottom rows to NaN

    return ds


def relative_humidity(t,qv,p):
        # ! convert specific humidity to mixing ratio
        mr = qv / (1-qv)
        # ! convert mixing ratio to vapor pressure
        e = mr * p / (0.62197+mr)
        # ! convert temperature to saturated vapor pressure
        es = 611.2 * np.exp(17.67 * (t - 273.15) / (t - 29.65))
        # ! finally return relative humidity
        relative_humidity = e / es

        # ! because it is an approximation things could go awry and rh outside or reasonable bounds could break something else.
        # ! alternatively air could be supersaturated (esp. on boundary cells) but cloud fraction calculations will break.
        # relative_humidity = min(1.0, max(0.0, relative_humidity))

        return relative_humidity


def add_relhum_to_ds(ds):
    """ calculate relative humidity at 2m from ta2m, psfc and hus2m. """

    #_______ calculate relative humidity at 2m: ________
    if dt == "3hr" and 'ta2m' in ds.data_vars :
        print("    calculating relative humidity" )
        # if static_P==True:
        #     # make a np array for the length of the input data with (static) pressure:
        #     P = np.repeat(dsP.psfc.values[np.newaxis, 12, :, :], len(ds.time), axis=0)
        # else: # if the data has surface pressure available:
        try:
            ds.psfc
        except AttributeError:
            print("no psfc in source dataset!")
        else:
            P = ds.psfc.values

        # calculate relative humidity:
        relhum = ds.hus2m.copy()
        relhum.values = relative_humidity(t=ds.ta2m.values, qv=ds.hus2m.values, p= P )
        # replace values over 1:
        relhum.values[relhum.values>1]=1.0
        # replace negative values
        relhum.values[relhum.values<0.0]=0.0
        ## if it is a dataArray we can add it like this:
        ds = ds.assign(relhum2m=relhum)
        ds.relhum2m.attrs['standard_name'] = "relative_humidity_2m"
        ds.relhum2m.attrs['units'] = "-"

    return ds


def get_livneh(icar_pcp):
    """get livneh data, cropped to the icar grid (defined by icar_pcp)"""

    t0 = time.time()
    print("\n- - - - - - - - - -   opening livneh files - - - - - - - - - - - - ")

    ### Livneh Precipitation [mm]
    files = glob.glob("/glade/campaign/ral/hap/common/Livneh_met_updated/precip/livneh_unsplit_precip.2021-05-02.19[8-9]*.nc")
    # files = glob.glob("/glade/p/ral/hap/common_data/Livneh_met_updated/precip/livneh_unsplit_precip.2021-05-02.19[8-9]*.nc")
    # files.extend(glob.glob("/glade/p/ral/hap/common_data/Livneh_met_updated/precip/livneh_unsplit_precip.2021-05-02.20*.nc"))
    # files.sort()
    print('   ',len(files), " Livneh precipiation files(years)" ) # the number of files (years)

    livneh = xr.open_mfdataset(files[0])  # need only one for the grid

    # # # crop livneh to ICAR bounds:
    buff=0.5
    max_lat=icar_pcp.lat.max().values+buff
    min_lat=icar_pcp.lat.min().values-buff
    max_lon=icar_pcp.lon.max().values+buff
    min_lon=icar_pcp.lon.min().values-buff

    LatIndexer, LonIndexer = 'lat', 'lon'

    livneh = livneh.sel(**{LatIndexer: slice(min_lat, max_lat ),
                            LonIndexer: slice(min_lon, max_lon)})

    livneh_pr   = livneh["PRCP"]#.load()

    # clean up some data; correct time dimension:
    if 'Time' in livneh_pr.dims:
        livneh_pr = livneh_pr.rename( {'Time':'time'})
    if not ('time' in livneh_pr.dims):
        print(' warning ' )

    print("   ", time.time() - t0)
    return livneh_pr


############################################
#                Main                      #
# ###########################################
if __name__ == '__main__':
    t00 = time.time()

    # process command line
    args = process_command_line()
    print('\n', args.model, args.scenario,  '\n')
    year            = args.year
    model           = args.model
    scenario        = args.scenario
    scen            = args.scenario.split('_')[0]  # drop the year from sspXXX_year
    dt              = args.dt    # timestep is either "daily" or "3hr"
    ICAR_nocp_path  = args.path_in
    ICAR_onLiv_path = args.path_out
    CMIP            = args.CMIP

    crop_size=3 ; crop=True  # crop outer n cells with NaN
    mask = True

    print(f"\n###############################################################\n")
    print(f"Regridding {dt} data from {args.path_in} to {args.path_out} \n on the livneh grid, for model {model} {scen} \n")
    if crop: print(f"   - cropping ICAR domain by {crop_size} cells on all sides to mask boundary effects")
    if mask: print(f"   - masking lakes in ta2m \n")
    print(f"###############################################################\n")

    # create out dir if it does not exist
    if not os.path.exists(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}"):
        try:
            os.makedirs(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}")
            print("   Created directory " + f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}")
        except FileExistsError:
            print('   outdir exists')


    if CMIP=="CMIP6":
        # file_prefix="ICAR_noGCMcp"
        file_prefix="*"
    elif CMIP=="CMIP5": # and dt=="3hr":
        file_prefix="icar_*"
        # file_prefix="ICAR_noGCMcp"  #CMIP5 with cp removed
    # elif CMIP=="CMIP5" and dt=="daily":
    #     file_prefix="icar_daily"  #CMIP5 daily
    else:
        print("   ! ! ! file_prefix unknown ! ! ! !")
    print(f"   {CMIP}, file_prefix: {file_prefix}")


    if dt=="3hr": # monthly or daily files, so 12 or ~ 365 per year

        print( f"opening {ICAR_nocp_path}/{model}_{scenario}/{dt}/{file_prefix}_{model}_{scen}_{year}*.nc")
        files = sorted(glob.glob(f"{ICAR_nocp_path}/{model}_{scenario}/{dt}/{file_prefix}_{model}_{scen}_{year}*"))  #  generic

        # - - - - - C. Define data to match:  (Livneh) - - - - -
        dsICAR1=xr.open_dataset(files[0]).load() # load 1 ICAR file for the regridder

        if 'precipitation' in dsICAR1.data_vars:
            pcp_var='precipitation'
        elif 'precip_dt' in dsICAR1.data_vars:
            pcp_var='precip_dt'
        else:
            print(" ERROR: Precipitation variable name unclear!  Stopping")
            sys.exit()

        # print(f" files[0]: {files[0]}")
        # print('\n  dsICAR1 shape' , dsICAR1[pcp_var].shape)
        # print( dsICAR1.isel(time=0) )

        livneh_pr = get_livneh(icar_pcp=dsICAR1.isel(time=0)).load()
        print('\n  livneh_pr shape' , livneh_pr.shape)

        # - - - - -  define regridder  - - - - - - - -
        # if crop:
        #     ICAR_grid_with_bounds = i2g.get_latlon_b(
        #     dsICAR1[pcp_var].isel(time=0).isel(
        #         lon_x=slice(crop_size,-crop_size)).isel(lat_y=slice(crop_size,-crop_size)),
        #         lon_str='lon',  lat_str='lat',
        #         lon_dim='lon_x', lat_dim='lat_y')
        # else:
        ICAR_grid_with_bounds = i2g.get_latlon_b(
                dsICAR1[pcp_var].isel(time=0),
                lon_str='lon',  lat_str='lat',
                lon_dim='lon_x', lat_dim='lat_y')

        livneh_grid_with_bounds = i2g.get_latlon_b_rect(livneh_pr,lon_str='lon',lat_str='lat',lon_dim='lon',lat_dim='lat')


        # regridder = xe.Regridder(livneh_grid_with_bounds, ICAR_grid_with_bounds, 'conservative')
        regridder2 = xe.Regridder( ICAR_grid_with_bounds, livneh_grid_with_bounds , 'conservative')


        # loop through months:
        for m in range(1,13):

            # # # # #   Skip the months that are overlap between the periods !!:   # # # # #
            # CMIP6 hist= till 2014-12; fut from 2015-01. -> Combine all simulations (2005 & 2050) at okt 1st
            # CMIP5 hist= till 2004-12; fut from 2005-01. -> Combine fut simulations (2050) at okt 1st
            # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
            if CMIP=="CMIP6" and scen=='hist' and int(year)==2005 and m>9 :
                print("skipping ", CMIP, scenario, year, ' month', m)
                continue
            elif (CMIP=="CMIP5" and scen=='historical' and int(year)==2005 and m>9):
                print("skipping ", CMIP, scenario, year, ' month', m)
                continue
            elif (CMIP=="CMIP6" and scenario[-5:]=='_2004' and int(year)==2005 and m<10):
                print("skipping ", CMIP, scenario, year, ' month', m)
                continue
            elif (CMIP=="CMIP5" and scenario[-10:]=='_2005_2050' and int(year)==2005 and m<10):
                print("skipping ", CMIP, scenario, year, ' month', m)
                continue
            elif (CMIP=="CMIP6" and scenario[-5:]=='_2004' and int(year)==2050 and m>9):
                continue
            elif (CMIP=="CMIP5" and scenario[-10:]=='_2005_2050' and int(year)==2050 and m>9):
                print("skipping ", CMIP, scenario, year, ' month', m)
                continue
            elif (CMIP=="CMIP6" and scenario[-5:]=='_2049' and int(year)==2050 and m<10):
                print("skipping ", CMIP, scenario, year, ' month', m)
                continue
            elif (CMIP=="CMIP5" and scenario[-10:]=='_2050_2100' and int(year)==2050 and m<10):
                print("skipping ", CMIP, scenario, year, ' month', m)
                continue

            files_m = glob.glob(f"{ICAR_nocp_path}/{model}_{scenario}/{dt}/{file_prefix}_{model}_{scen}_{year}-{str(m).zfill(2)}*.nc")
            print(f"   opening {ICAR_nocp_path}/{model}_{scenario}/{dt}/{file_prefix}_{model}_{scen}_{year}-{str(m).zfill(2)}*.nc")

            # -------  Load ICAR and crop to remove boundary effects:  -----
            # dsICAR=xr.open_mfdataset(files_m).load()
            dsICAR = xr.open_mfdataset(files_m)

            # - - -   add relative humidity to ds - - -
            if calc_relhum:
                dsICAR = add_relhum_to_ds( dsICAR )

            # - - - - mask lakes  - - - -
            """I would recommend a fill 1 grid cell in all directions first to minimize the distance you are filling from, then a fill W->E all the way across to fill in all the lakes.  Then when it is regridded, you will be regridding to a dataset (livneh) that doesn't have "lakes" in it to begin with other than great salt lake, so if people use the data, they are mosly thinking of it as "land" air temperatures anyway. But we should have a value at all the gridcells the Livneh dataset as a value."""
            if CMIP=="CMIP6" and mask:
                geo = xr.open_dataset(geo_file)
                msk = (geo.isel(Time=0).LANDMASK==1).values
                # dsICAR = dsICAR.where(msk)
                if 'lon_x' in dsICAR.dims: londim='lon_x'
                if 'lon' in dsICAR.dims: londim='lon'
                if 'lat_y' in dsICAR.dims: latdim='lat_y'
                if 'lat' in dsICAR.dims: latdim='lat'
                masked_ta2m = dsICAR['ta2m'].where(msk)
                masked_ta2m = masked_ta2m.ffill(londim,limit=1).bfill(londim,limit=1).ffill(latdim,limit=1).bfill(latdim,limit=1).ffill(londim)
                dsICAR['ta2m'] = masked_ta2m

            # - - - - - crop boundary  - - - - -
            if crop:
                # dsICAR = dsICAR.isel(lon_x=slice(crop_size,-crop_size)).isel(lat_y=slice(crop_size,-crop_size))

                # Set outer cells to NaN iso of cropping:
                dsICAR = crop_nan(dsICAR, crop_size)

                # print(f" After cropping {crop_size} cells: dsICAR['lon'].shape is ",dsICAR['lon'].shape )
                print(f"   masking lakes in ta2m")
            dsICAR = dsICAR.load()


            # ---------  Regrid ICAR to livneh grid:    -----------------
            print("   regridding ICAR to livneh grid.....")
            t0 = time.time()
            icar_on_liv = regridder2(dsICAR)
            print("   ", time.time() - t0)

            # -----  mask values outside livneh domain (like waterbodies) (in ta2m) -----
            mask2= ~livneh_pr.isel(time=0).isnull()
            icar_on_liv['ta2m'] = icar_on_liv['ta2m'].where(mask2)

            # in all vars??
            # this will be done in bias correction anyway??

            # -----  write variable attributes  ------------
            for var in icar_on_liv.data_vars:
                try:
                    icar_on_liv[var].attrs = dsICAR[var].attrs
                except:
                    print(f"  could not write/read {var} attrs")
                    continue

            # -------  save (as monthly file) -------
            if not os.path.exists(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}"):
                os.makedirs(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}")

            outfile= f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}/icar_{dt}_livgrd_{model}_{scen}_{year}-{str(m).zfill(2)}.nc"
            icar_on_liv.to_netcdf( outfile )
            print(f"   written to {outfile} \n")


    ##### daily ####
    elif dt=="daily": # monthly or daily files, so 12 or ~ 365 per year
        # if int(year)==2005 and scen=="hist":
        #     files = glob.glob(f"{ICAR_nocp_path}/{model}_{scenario}/{dt}/{file_prefix}_{model}_{scen}_{year}-0*.nc")  #

        files = glob.glob(f"{ICAR_nocp_path}/{model}_{scenario}/{dt}/{file_prefix}_{model}_{scen}_{year}*.nc")  #  generic

        print("\n- - - - - - - - - -   opening ICAR file(s) to correct - - - - - - - - - - - - ")
        # print(files)
        # open the whole year at once:
        dsICAR=xr.open_mfdataset(files)

        if CMIP=="CMIP6" and scen=='hist' and int(year)==2005:
            dsICAR.sel(time=slice(None,"2005-09-30"))
        elif CMIP=="CMIP5" and scen=='historical' and int(year)==2005:
            dsICAR.sel(time=slice(None,"2005-09-30"))
        elif (CMIP=="CMIP6" and scenario[-5:]=='_2004' and int(year)==2005 ):
            dsICAR.sel(time=slice("2005-10-01", None))
        elif (CMIP=="CMIP5" and scenario[-10:]=='_2005_2050' and int(year)==2005 ):
            # Start on 2005-01-01 or 10-01 for CMIP5???
            dsICAR.sel(time=slice("2005-10-01", None))
            # ????
        elif (CMIP=="CMIP6" and scenario[-5:]=='_2004' and int(year)==2050):
            dsICAR.sel(time=slice(None,"2050-09-30"))
        elif (CMIP=="CMIP5" and scenario[-10:]=='_2005_2050' and int(year)==2050):
            dsICAR.sel(time=slice(None,"2050-09-30"))
        elif (CMIP=="CMIP6" and scenario[-5:]=='_2049' and int(year)==2050):
            dsICAR.sel(time=slice("2050-10-01", None))
        elif (CMIP=="CMIP5" and scenario[-10:]=='_2050_2100' and int(year)==2050):
            dsICAR.sel(time=slice("2050-10-01", None))

        livneh_pr = get_livneh(icar_pcp=dsICAR.isel(time=0)).load() # get before cropping ICAR


        # - - - - mask lakes  - - - -
        if CMIP=="CMIP6" and mask:
            geo = xr.open_dataset(geo_file)
            msk = (geo.isel(Time=0).LANDMASK==1).values
            # dsICAR = dsICAR.where(msk)
            if 'lon_x' in dsICAR.dims: londim='lon_x'
            if 'lon' in dsICAR.dims: londim='lon'
            if 'lat_y' in dsICAR.dims: latdim='lat_y'
            if 'lat' in dsICAR.dims: latdim='lat'

            masked_Tmin = dsICAR['Tmin'].where(msk)
            masked_Tmin = masked_Tmin.ffill(londim,limit=1).bfill(londim,limit=1).ffill(latdim,limit=1).bfill(latdim,limit=1).ffill(londim)
            dsICAR['Tmin'] = masked_Tmin

            masked_Tmax = dsICAR['Tmax'].where(msk)
            masked_Tmax = masked_Tmax.ffill(londim,limit=1).bfill(londim,limit=1).ffill(latdim,limit=1).bfill(latdim,limit=1).ffill(londim)
            dsICAR['Tmax'] = masked_Tmax

            print(f"   masking lakes in Tmin/Tmax")
        # - - - - -   crop domain (set to nan)  - - - -
        if crop:
            dsICAR = crop_nan(dsICAR, crop_size)
            # dsICAR = dsICAR.isel(lon_x=slice(crop_size,-crop_size)).isel(lat_y=slice(crop_size,-crop_size))
            # print(f" After cropping {crop_size} cells: dsICAR['lon'].shape is ",dsICAR['lon'].shape )
        dsICAR = dsICAR.load()


        if 'precipitation' in dsICAR.data_vars:
            pcp_var='precipitation'
        elif 'precip_dt' in dsICAR.data_vars:
            pcp_var='precip_dt'
        elif 'Prec' in dsICAR.data_vars:
            pcp_var='Prec'
        else:
            print(" ERROR: Precipitation variable name unclear!  Stopping")
            sys.exit()

        # - - - - -  define regridder  - - - - - - - -
        ICAR_grid_with_bounds = i2g.get_latlon_b(
            dsICAR[pcp_var].isel(time=0),
            lon_str='lon',  lat_str='lat',
            lon_dim='lon_x', lat_dim='lat_y')



        # - - - - - C. Define data to match:  (Livneh) - - - - -


        livneh_grid_with_bounds = i2g.get_latlon_b_rect(livneh_pr,lon_str='lon',lat_str='lat',lon_dim='lon',lat_dim='lat')

        # regridder = xe.Regridder(livneh_grid_with_bounds, ICAR_grid_with_bounds, 'conservative')
        regridder2 = xe.Regridder( ICAR_grid_with_bounds, livneh_grid_with_bounds , 'conservative')


        # ---------  Regrid ICAR to livneh grid:    -----------------
        print("\n - - - - - - - - - - -   regridding ICAR to livneh grid.....  - - - - - - - - - - ")
        t0 = time.time()
        icar_on_liv = regridder2(dsICAR)
        print("   ", time.time() - t0)

        # -----  mask values outside livneh domain (like waterbodies) (in ta2m) -----
        mask2= ~livneh_pr.isel(time=0).isnull()
        icar_on_liv['Tmin'] = icar_on_liv['Tmin'].where(mask2)
        icar_on_liv['Tmax'] = icar_on_liv['Tmax'].where(mask2)

        # -------  save (as monthly file) -------
        if not os.path.exists(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}"):
            os.makedirs(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}")

        outfile= f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}/icar_{dt}_livgrd_{model}_{scen}_{year}.nc"
        icar_on_liv.to_netcdf( outfile )
        print(f"written to {outfile} \n")

    print(f"\n - - -   {dt} {model} {scen} {year} done! - - -")