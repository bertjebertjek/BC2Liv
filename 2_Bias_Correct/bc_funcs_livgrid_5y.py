# # #    functions for bias correcting ICAR 2D CMIP runs    # # # # #
#  - generic version, no regridding. Takes timestep (1- or 3-hourly input data),
#    and uses daily Obs data to quantile map.
#
#   Reference data has what timestep?
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
# from datetime import datetime
# import glob, os
import xarray as xr
import dask
import numpy as np
from random import randrange
import gc
import sys
# sys.path.append('/glade/u/home/bkruyt/libraries/storylines/storylines')
sys.path.append('/glade/u/home/bkruyt/libraries')
sys.path.append('/glade/u/home/bkruyt/libraries/st_lines')
sys.path
# import icar2gmet as i2g

# from storylines.tools import quantile_mapping
### If storylines gives an error , use py3_yifan kernel /conda env
# alternatively ../Example_scripts/2D_quantile_map/quantile_mapping.py

import quantile_mapping # from same folder.

# verbose=True # print more details on runtime

noise_path  = "/glade/derecho/scratch/bkruyt/CMIP6"
NOISE_u     = xr.open_dataset(f"{noise_path}/uniform_noise_480_480.nc" )
noise_val   = 0.01

######################################################
############       FUNCTIONS           ###############
######################################################

def add_noise(df, noise_val=noise_val, random=True):
    """Add uniform noise to a 2D dataset df."""

    with dask.config.set(**{'array.slicing.split_large_chunks': True}):

        u_noise = NOISE_u.uniform_noise  #.load() # 55000 x 480 x480

        # Add noise to (daily) input data:
        # to avoid taking the same noise values every time, we add a random int.
        t = len(df.time)
        if random:
            r = randrange(1000)
        else:
            r = 0
        noise_arr = noise_val * u_noise[r:(r+t) , :df.shape[1], :df.shape[2] ]
        df = xr.where( df>0, df + noise_val, noise_arr.values)

        return df


######################################################
###############    PRECIPITATION    ##################
######################################################

def correct_precip(this_ds, dsObs, dsRef,  bc_by_month=False, noise=True, verbose=False):
    """ this ds and dsRef: datasets with the same precipiation variable, dsObs is DataArray with observations ('data to match') """
    t0 = time.time()

    if bc_by_month:
        print("   - - - - - - - -  Bias-correcting precipitation by month - - - - - - - - -  ")
    else:
        print("   - - - - - - - -  Bias-correcting precipitation by year  - - - - - - - - -  ")

    # determine timestep:
    tdelta_int = int(this_ds.time.dt.hour[1]-this_ds.time.dt.hour[0])
    if tdelta_int ==0:
        tdelta_int = int(this_ds.time.dt.hour[2]-this_ds.time.dt.hour[1])
        if tdelta_int ==0:
            tdelta_int=24
            print("       tdelta_int=0 ; setting tdelta_int=24; daily timestep")
    print(f"   timestep of input data is {tdelta_int} hrs")

    # determine the name of the hourly precip var:  (make function)
    if 'precip_hr' in this_ds.data_vars:
        pcp_var = 'precip_hr'
    elif  'precip_dt' in this_ds.data_vars:
        pcp_var = 'precip_dt'
    elif  'Prec' in this_ds.data_vars:
        pcp_var = 'Prec'
    elif  'precipitation' in this_ds.data_vars:
        pcp_var = 'precipitation'
    else:
        print( 'define time-step precipitation variable!' )

    t0 = time.time()

    print("   loading data")

    try: # pcp_var in this_ds.data_vars:
        da_pcp    =  this_ds[pcp_var]
    except:
        da_pcp    =  this_ds

    # dsObs =  dsObs#.load()

    print("      ", time.time() - t0)

    if verbose:
        try:
            print(f"      precipitation variable is {pcp_var}")
            print(f"      max input precipitation value is {np.nanmax(da_pcp.values) } kg m-2")
            print(f"      max obs. precipitation value is {np.nanmax(dsObs.values) } {dsObs.units}")
        except:
            print(f"      precipitation variable is {pcp_var}")

    t0 = time.time()
    if tdelta_int<24:
        print("   making daily precip")
        daily = da_pcp.resample(time='1D').sum(dim='time').load()
    else:
        daily = da_pcp.load()
    print("      ", time.time() - t0)


    #______________ add noise _____________
    t0 = time.time()
    # Add noise to (daily) input and Reference data:
    if noise:
        print(f"   adding noise to input and ref data with max val {noise_val}")
        daily_n = add_noise(daily, noise_val=noise_val, random=True)
        dsRef_n = add_noise(dsRef, noise_val=noise_val, random=True)
    else:
        daily_n = daily
        dsRef_n = dsRef
        print("   ! ! !  NOT adding noise to input and ref data  ! ! !")

    print("      ", time.time() - t0)



    # _____________    Do the bias correction   _____________
    t0 = time.time()
    print("   quantile mapping")

    # NB: (very small) negative values are introduced by the bc procedure when setting extrapolate='1to1'. (This does not happen when dsRef=None. ). To prevent this we set extrapolate to max. (2024-04-18)
    if bc_by_month:
        bc_daily = quantile_mapping.quantile_mapping_by_group(
            # def quantile_mapping_by_group(input_data, ref_data, data_to_match, (ref should exclude 5y input)
            daily_n,  dsRef_n, dsObs, # correct to liv grid
            grouper='time.month', detrend=False,  use_ref_data=True,# use_ref_data is not used! set ref_data=None to exclude ref data!
            extrapolate="max", n_endpoints=50
            )
    else:  #def quantile_mapping(input_data, ref_data, data_to_match,...
        bc_daily = quantile_mapping.quantile_mapping(
            daily_n, dsRef_n, dsObs,  #daily, dsRef, dsObs,#
            detrend=False,  use_ref_data=True,
            extrapolate="max", n_endpoints=50
            )

    print("      ", time.time() - t0)

    if np.count_nonzero(bc_daily.values<0) >0:
        print(f"\n ! ! !   {np.count_nonzero(daily_n.values<0)} negative precip values before bc ! ! ! ")
        print(f" ! ! !   {np.count_nonzero(bc_daily.values<0)} negative precip values after bc ! ! ! ")
        print(f" ! ! !   {np.nanmin(bc_daily.values)} minimum precip value after bc ! ! ! \n")
    t0 = time.time()

    if verbose:
        # print("      da_pcp.isnull():",np.sum(da_pcp.isnull().values))
        print("      da_pcp.shape:",da_pcp.shape)
        print("      da_pcp == 0 :",np.sum(da_pcp.values==0))


    #_______ apply correction to n-hourly data: ______
    print("   applying correction to ",tdelta_int,"-hourly data")

    # Calculate the correction array
    correction = bc_daily.values / daily_n.values

    # repeat for nr of timesteps per day:
    correction =  np.repeat(correction, (24 / tdelta_int), axis=0)

    if np.count_nonzero(correction<0) >0 : print(f"\n ! ! !   {np.count_nonzero(correction<0)} negative correction values  ! ! ! \n")

    # Create a mask for elements where daily sum is zero and bc_daily is positive
    mask = (daily == 0) & (bc_daily > 0)
    mask = np.repeat(mask.values, (24 / tdelta_int), axis=0)
    #__ Set values to scaled bc_daily where daily sum is zero and bc_daily is positive __
    #   method='zero': when resampled to daily again, the values 'line up"
    bc_tdelta =  bc_daily.interp(time=da_pcp.time, method='zero') /(24 / tdelta_int)

    if verbose:
        print(f"      mask shape: {mask.shape}")
        print(f"      correction shape: {correction.shape} ") # \n
        print(f"      bc_tdelta shape: {bc_tdelta.shape}")
        # print(f"\n   bc_daily.time: {bc_daily.time}" )
        # print(f"   bc_tdelta.time: {bc_tdelta.time}" )
        # print(f"\n   bc_daily.values[:12]: {bc_daily.values[:12,150,150]}" )
        # print(f"   bc_tdelta.values[:12]: {bc_tdelta.values[:12, 150,150]}" )
        # print(f"\n   sum(bc_daily.sel(time=slice(1950-01-01,1950-01-10)).values): {sum(bc_daily.sel(time=slice('1950-01-01','1950-01-10'))[:,150,150].values)}" )
        # print(f"   sum(bc_tdelta.sel(time=slice('1950-01-01','1950-01-10')).values):{sum(bc_tdelta.sel(time=slice('1950-01-01','1950-01-10'))[:,150,150].values)}" )


    # ______ Apply correction to (all) values ______
    da_pcp *= correction[:len(da_pcp.time)] #subset to catch the possibility of the last year being not complete

    # except where the original daily value was 0, here we set to the scaled bc_daily value:
    da_pcp = da_pcp.where(~mask[:len(da_pcp.time)], bc_tdelta)

    if verbose:
        print("      da_pcp.shape():",da_pcp.shape)
        print("      da_pcp == 0 :",np.sum(da_pcp.values==0))
        print("      da_pcp.isnull():",np.sum(da_pcp.isnull().values))
        print("      ", time.time() - t0)

    return this_ds



######################################################
###############    TEMPERATURE    ####################
######################################################


def correct_temperature( this_ds, dsObs_tmin, dsObs_tmax, dsRef_tmin, dsRef_tmax,  bc_by_month=False):

    if bc_by_month:
        print("   - - - - - - - -  Bias-correcting temperature by month - - - - - - - - -  ")
    else:
        print("   - - - - - - - -  Bias-correcting temperature by year  - - - - - - - - -  ")

    # determine time step:
    tdelta_int = int(this_ds.time.dt.hour[1]-this_ds.time.dt.hour[0])
    if tdelta_int ==0:
        tdelta_int = int(this_ds.time.dt.hour[2]-this_ds.time.dt.hour[1])
        mean_tdelta = np.mean(this_ds.time.dt.hour)
        print(f"   mean delta t: {np.round(mean_tdelta,2) }")
        if tdelta_int ==0:
            tdelta_int=24
            print("       tdelta_int=0 ; setting tdelta_int=24; daily timestep")
    print(f"   timestep of input data is {tdelta_int} hrs")

    t0 = time.time()
    print("   loading temperature")
    if 'ta2m' in this_ds.data_vars:
        ta2m = this_ds["ta2m"].load()
        if np.isnan(ta2m).any():
            print('ta2m has nans! ')

        print("   making daily tmin/tmax values from ta2m")
        tmin = ta2m.resample(time='1D').min(dim='time').load()
        tmax = ta2m.resample(time='1D').max(dim='time').load()

        if np.isnan(tmin).any():
            print('tmin has nans! ')
        if np.isnan(tmax).any():
            print('tmax has nans! ')

    elif'Tmin' in this_ds.data_vars:
        print("   found tmin/tmax values ")
        tmin = this_ds["Tmin"].load()
        if np.isnan(tmin).any():
            print('tmin has nans! ')

        tmax = this_ds["Tmax"].load()
        if np.isnan(tmax).any():
            print('tmax has nans! ')
    else:
        print(" Unclear which temperature variable to bias-correct. Stopping.")
        exit()


    dsRef_tmin = dsRef_tmin#.load()
    dsRef_tmax = dsRef_tmax#.load()
    dsObs_tmin = dsObs_tmin#.load() # should already be loaded
    dsObs_tmax = dsObs_tmax#.load()


    print("      ", np.round(time.time() - t0, 2))

    t0 = time.time()
    if bc_by_month: ## NB def quantile_mapping_by_group(input_data, ref_data, data_to_match, grouper='time.month', **kwargs):
        print("   quantile mapping t_min")
        bc_tmin = quantile_mapping.quantile_mapping_by_group(
            # tmin, icar_tmin, ltmin_on_icar,  extrapolate="1to1", grouper='time.month' , n_endpoints=50
             tmin, dsRef_tmin, dsObs_tmin,  extrapolate="1to1", grouper='time.month' , n_endpoints=50
            )
        del dsRef_tmin, dsObs_tmin

        print("   quantile mapping t_max")
        bc_tmax = quantile_mapping.quantile_mapping_by_group(
            tmax, dsRef_tmax, dsObs_tmax,  extrapolate="1to1", grouper='time.month' , n_endpoints=50
            )
        del dsRef_tmax, dsObs_tmax
    else:
        print("   quantile mapping t_min")
        bc_tmin = quantile_mapping.quantile_mapping(
            tmin, dsRef_tmin, dsObs_tmin,  extrapolate="1to1" , n_endpoints=50
            )
        del dsRef_tmin, dsObs_tmin

        print("   quantile mapping t_max")
        bc_tmax = quantile_mapping.quantile_mapping(
             tmax, dsRef_tmax, dsObs_tmax,  extrapolate="1to1" , n_endpoints=50
            )
        del dsRef_tmax, dsObs_tmax

    print("   ", np.round(time.time() - t0, 2))

    # cleanup
    gc.collect()

    if np.isnan(bc_tmin).any():
        print('bc_tmin has nans! ')
    if np.isnan(bc_tmax).any():
        print('bc_tmax has nans! ')


    t0 = time.time()
    print("   applying correction to "+str(tdelta_int)+"-hourly data")
    if 'ta2m' in this_ds.data_vars:
        for i in range(len(this_ds.time)):
            t = int(i/(24/tdelta_int))

            dtr = (tmax[t] - tmin[t]).values
            dtr[dtr<0.001] = 0.001
            this_ds["ta2m"][i] -= tmin[t]
            this_ds["ta2m"][i] /= dtr
            this_ds["ta2m"][i] *= np.abs(bc_tmax[t] - bc_tmin[t])
            this_ds["ta2m"][i] += bc_tmin[t]+273.15
    elif 'Tmin' in this_ds.data_vars:
        if tdelta_int==24:
            print('   Tmin min before:', np.nanmin(this_ds['Tmin'].values))
            print('   Tmax max before:', np.nanmax(this_ds['Tmax'].values))
            this_ds['Tmin'].values = bc_tmin
            this_ds['Tmax'].values = bc_tmax
            print('   Tmin min after bc:', np.nanmin(this_ds['Tmin'].values))
            print('   Tmax max after bc:', np.nanmax(this_ds['Tmax'].values))
        else:
            print(f"\n   ! ! ! tdelta_int= {tdelta_int}  but no ta2m in data_vars. Stopping. ! ! !")
            sys.exit()
            # for i in range(len(this_ds.time)):  # Quick fix, revisit?
            #     t = int(i/(24/tdelta_int))
            #     this_ds['Tmin'].values[i] *= bc_tmin[t]/this_ds['Tmin'].values[i]
            #     this_ds['Tmax'].values[i] *= bc_tmax[t]/this_ds['Tmax'].values[i]


    print("   ", np.round(time.time() - t0, 2))

    return this_ds