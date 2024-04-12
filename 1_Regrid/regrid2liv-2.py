#!/usr/bin/env python
# coding: utf-8
############################################################################################
#
# Regrid ICAR to the Livneh grid
#  - make sure GCM cp is removed first(!)
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
# # # # # ICAR daily precip:
# ICAR_day_path = '/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_3h_pcpfix'

# # # # ICAR daily;  precip w/o convective (output):
# ICAR_nocp_path = '/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_nocp_full' # CMIP6
# ICAR_nocp_path = "/glade/campaign/ral/hap/currierw/icar/output" # CMIP5, includes GCM cp!!

#### OUTPUT: (now argument in .sh script) ####
# ICAR_onLiv_path =  '/glade/scratch/bkruyt/CMIP6/WUS_icar_LivGrd'
# ICAR_onLiv_path =  '/glade/scratch/bkruyt/CMIP5/WUS_icar_LivGrd_incGCMcp'

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

    # livneht = livneht.sel(**{LatIndexer: slice(min_lat, max_lat ),
    #                         LonIndexer: slice(min_lon, max_lon)})

    livneh_pr   = livneh["PRCP"]#.load()
    # livneh_tmin = livneht["Tmin"]#.load()
    # livneh_tmax = livneht["Tmax"]#.load()

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

    crop_size=3 ; crop=True  # exaggerate for test
    mask = True

    print(f"\n###############################################################\n")
    print(f"Regridding {dt} data from {args.path_in} to {args.path_out} \n on the livneh grid, for model {model} {scen} \n")
    if crop: print(f"   - cropping ICAR domain by {crop_size} cells on all sides to mask boundary effects")
    if mask: print(f"   - masking lakes \n")
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
        files = glob.glob(f"{ICAR_nocp_path}/{model}_{scenario}/{dt}/{file_prefix}_{model}_{scen}_{year}*.nc")  #  generic

        # - - - - - C. Define data to match:  (Livneh) - - - - -
        dsICAR1=xr.open_dataset(files[0]).load() # load 1 ICAR file for the regridder

        if 'precipitation' in dsICAR1.data_vars:
            pcp_var='precipitation'
        elif 'precip_dt' in dsICAR1.data_vars:
            pcp_var='precip_dt'
        else:
            print(" ERROR: Precipitation variable name unclear!  Stopping")
            sys.exit()

        print(f" files[0]: {files[0]}")
        print('\n  dsICAR1 shape' , dsICAR1[pcp_var].shape)
        print( dsICAR1.isel(time=0) )
        livneh_pr = get_livneh(icar_pcp=dsICAR1.isel(time=0)).load()
        print('\n  livneh_pr shape' , livneh_pr.shape)

        # - - - - -  define regridder  - - - - - - - -
        if crop:
            ICAR_grid_with_bounds = i2g.get_latlon_b(
            dsICAR1[pcp_var].isel(time=0).isel(
                lon_x=slice(crop_size,-crop_size)).isel(lat_y=slice(crop_size,-crop_size)),
                lon_str='lon',  lat_str='lat',
                lon_dim='lon_x', lat_dim='lat_y')
        else:
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

            # - - - - mask lakes  - - - -
            if CMIP=="CMIP6" and mask:
                geo = xr.open_dataset(geo_file)
                msk = (geo.isel(Time=0).LANDMASK==1).values
                dsICAR = dsICAR.where(msk)
            # - - - - - crop boundary  - - - - -
            if crop:
                dsICAR = dsICAR.isel(lon_x=slice(crop_size,-crop_size)).isel(lat_y=slice(crop_size,-crop_size))
                # print(f" After cropping {crop_size} cells: dsICAR['lon'].shape is ",dsICAR['lon'].shape )
            dsICAR = dsICAR.load()


            # ---------  Regrid ICAR to livneh grid:    -----------------
            print("   regridding ICAR to livneh grid.....")
            t0 = time.time()
            icar_on_liv = regridder2(dsICAR)
            print("   ", time.time() - t0)

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
            dsICAR = dsICAR.where(msk)
        if crop:
                dsICAR = dsICAR.isel(lon_x=slice(crop_size,-crop_size)).isel(lat_y=slice(crop_size,-crop_size))
                print(f" After cropping {crop_size} cells: dsICAR['lon'].shape is ",dsICAR['lon'].shape )
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

        # -------  save (as monthly file) -------
        if not os.path.exists(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}"):
            os.makedirs(f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}")

        outfile= f"{ICAR_onLiv_path}/{model}_{scenario}/{dt}/icar_{dt}_livgrd_{model}_{scen}_{year}.nc"
        icar_on_liv.to_netcdf( outfile )
        print(f"written to {outfile} \n")

    print(f"\n - - -   {dt} {model} {scen} {year} done! - - -")