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

noise_path = "/glade/derecho/scratch/bkruyt/CMIP6"

######################################################
###############    PRECIPITATION    ##################
######################################################

def correct_precip(this_ds, dsObs, dsRef,  bc_by_month=False):
    """ this ds and dsRef: datasets with the same precipiation variable, dsObs is DataArray"""
    t0 = time.time()

    if bc_by_month:
        print("   - - - - - - - -  Bias-correcting precipitation by month - - - - - - - - -  ")
    else:
        print("   - - - - - - - -  Bias-correcting precipitation by year  - - - - - - - - -  ")


    # # # check if time is cf.noleap weirdness and if so, correct:
    # # print("   time calendar: ", this_ds.time.encoding['calendar'])
    # try: # in case calendar is 'proleptic_gregorian'
    #     (this_ds.time.values.min().astype('datetime64[D]'), this_ds.time.values.max().astype('datetime64[D]'))
    #     tdelta_int =  int(            int(this_ds.time[1].values-this_ds.time[0].values ) /3600 / 1e9         )
    # except:  # in case calendar is 'noleap'
    #     time_cor = this_ds.indexes['time'].to_datetimeindex() # by not overwriting we prevent the encoding from going crazy?
    #     tdelta = (time_cor[1]-time_cor[0]) #.astype('timedelta64[h]')
    #     tdelta_int = int(tdelta.seconds/ 3600)

    tdelta_int = int(this_ds.time.dt.hour[1]-this_ds.time.dt.hour[0])
    if tdelta_int ==0:
        tdelta_int = int(this_ds.time.dt.hour[2]-this_ds.time.dt.hour[1])
        if tdelta_int ==0:
            tdelta_int=24
            print("       tdelta_int=0 ; setting tdelta_int=24; daily timestep")
    print(f"   timestep of input data is {tdelta_int} hrs")

    # determine the name of the hourly precip var:  (could also make it an argument)
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
    # print("   loading timestep precip")
    print("   loading data")

    try: # pcp_var in this_ds.data_vars:
        pr    =  this_ds[pcp_var].load()
    except:
        pr    =  this_ds.load()

    # dsObs =  dsObs#.load()

    ## dsRef should be dataArray, not dataset
    # if pcp_var in dsRef.data_vars:
    #     dsRef =  dsRef[pcp_var].load()  # should already be loaded, but calling load twice is ok (?)
    # else:
    #     dsRef =  dsRef
        # print(" ! ! !  Error: precipitation variable not the same in Ref and input datasets! Stopping")
        # sys.exit()

    print("      ", time.time() - t0)

    try:
        print(f"      precipitation variable is {pcp_var}")
        print(f"      max input precipitation value is {np.nanmax(pr.values) } kg m-2")
        print(f"      max obs. precipitation value is {np.nanmax(dsObs.values) } {dsObs.units}")
    except:
        print(f"      precipitation variable is {pcp_var}")

    t0 = time.time()
    print("   making daily precip")
    daily = pr.resample(time='1D').sum(dim='time').load()
    print("      ", time.time() - t0)


    #______________ add noise _____________
    t0 = time.time()
    print("   adding noise to input and ref data")
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):

        NOISE_u = xr.open_dataset(f"{noise_path}/uniform_noise_480_480.nc" )
        u_noise = NOISE_u.uniform_noise  #.load() # 55000 x 480 x480
        noise_val= 0.01

        # Add noise to (daily) input data:
        # to avoid taking the same noise values every time, we add a random int.
        t = len(daily.time)
        r = randrange(1000)
        noise_arr = noise_val * u_noise[r:(r+t) , :daily.shape[1], :daily.shape[2] ]
        daily = xr.where( daily>0, daily + noise_val, noise_arr.values)

        # Also add noise to ref data:
        t = len(dsRef.time)
        r = randrange(1000) # use a different random (r) value for Ref(?)
        noise_arr = noise_val * u_noise[r:(r+t) , :daily.shape[1], :daily.shape[2] ]
        # # only the values that are 0 should get noise added, the values that aren’t 0 should get 0.1 (or whatever) added to them
        dsRef = xr.where( dsRef>0, dsRef + noise_val, noise_arr.values)

    print("      ", time.time() - t0)



    # _____________    Do the bias correction   _____________
    t0 = time.time()
    print("   quantile mapping")
    if bc_by_month:
        bc_daily = quantile_mapping.quantile_mapping_by_group(
            # def quantile_mapping_by_group(input_data, ref_data, data_to_match, (ref should exclude 5y input)
            daily,  dsRef, dsObs, # correct to liv grid
            grouper='time.month', detrend=False,  use_ref_data=True,# use_ref_data is not used! set ref_data=None to exclude ref data!
            extrapolate="1to1", n_endpoints=50
            )
    else:  #def quantile_mapping(input_data, ref_data, data_to_match,...
        bc_daily = quantile_mapping.quantile_mapping(
            daily, dsRef, dsObs,
            detrend=False,  use_ref_data=True,
            extrapolate="1to1", n_endpoints=50
            )
        # bc_daily = quantile_mapping.qmap_grid(daily, dsObs, icar_pr, extrapolate="1to1")  #input_data, ref_data, data_to_match,
    print("      ", time.time() - t0)

    t0 = time.time()
    print("   applying correction to ",tdelta_int,"-hourly data")
    for i in range(len(this_ds.time)):
        t = int(i/(24/tdelta_int))
        correction = bc_daily[t] / daily[t]
        correction.values[daily.values[t] == 0] = 0
        # if (i%24)==0: print(i, int(i/24), correction.values[150,60], daily.values[int(i/24), 150, 60])

        this_ds[pcp_var][i] *= correction

    # clean up (is this needed?)
    # del daily,  dsRef, dsObs
    # gc.collect()
    print("      this_ds[pcp_var].shape():",this_ds[pcp_var].shape)
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

    # tdelta_int = int(this_ds.time.dt.hour[1]-this_ds.time.dt.hour[0])
    # if tdelta_int ==0:
    #     tdelta_int=24
    #     print("       tdelta_int=24; daily timestep")
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

        # if tdelta_int==24:
        print("   making daily tmin/tmax values from ta2m")
        tmin = ta2m.resample(time='1D').min(dim='time').load()
        tmax = ta2m.resample(time='1D').max(dim='time').load()
        # # elif tdelta_int==3:
        # else:
        #     # print(f"   making {tdelta_int}h tmin/tmax values from ta2m")
        #     print(f"   making daily tmin/tmax values from ta2m")
        #     tmin = ta2m.resample(time='1D').min(dim='time').load()
        #     tmax = ta2m.resample(time='1D').max(dim='time').load()

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

            dtr = np.abs(tmax[t] - tmin[t]).values
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
            print("   ! ! !  Need to think about this some more   ! ! !")  # Quick fix, revisit?
            for i in range(len(this_ds.time)):
                t = int(i/(24/tdelta_int))
                this_ds['Tmin'].values[i] *= bc_tmin[t]/this_ds['Tmin'].values[i]
                this_ds['Tmax'].values[i] *= bc_tmax[t]/this_ds['Tmax'].values[i]


    print("   ", np.round(time.time() - t0, 2))

    return this_ds