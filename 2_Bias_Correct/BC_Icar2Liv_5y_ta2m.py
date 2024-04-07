#############################################################################
#
#  Bias Correction of x-hourly ICAR data to the Livneh Grid.
#
#   Temperature
#
#   - corrects ta2m by calculating tmin, tmax, trange. Livneh Tmin & Tmax for Obs.
#   - in 5 year blocks, leaves out the 5 years from ref data. (historical GCM-icar runs)
#   - 2024-04-03: reads in 3hr ref data,makes daily Tmin/Tmax.
#
#
# Bert Kruyt, NCAR RAL, october 2023
#############################################################################

import argparse
import time
from datetime import datetime
import glob, os
import xarray as xr
import numpy as np
import multiprocessing as mp
import psutil
# the bc functions:
from bc_funcs_livgrid_5y import *
import sys
# sys.path.append('/glade/u/home/bkruyt/libraries/storylines/storylines')
sys.path.append('/glade/u/home/bkruyt/libraries')
sys.path.append('/glade/u/home/bkruyt/libraries/st_lines')
sys.path
import icar2gmet as i2g
# from storylines.tools import quantile_mapping # not used here but in bc_funcs.py
### If storylines gives an error , use py3_yifan kernel /conda env
# alternatively ../Example_scripts/2D_quantile_map/quantile_mapping.py
import warnings
warnings.simplefilter("ignore", category=Warning) #SerializationWarning ?


############################################
#                Settings
############################################
bc_by_month=True

test = False # reduces datasets for faster processing, incorrect results!

calc_relhum = True  # add a relative humidity variable to the dataset, by calculating this from (corrected) ta2m and qv

dropvars=True   # drop variables in vars_to_drop (below). Also writes to "3hr" subdir iso "3hr_ta2m" !

# the variables to remove:
vars_to_drop=["swe", "soil_water_content", "hfls", "hus2m",
            "runoff_surface","runoff_subsurface",
            "soil_column_total_water","soil_column_total_water",
            "ivt","iwv","iwl","iwi", "snowfall_dt", "cu_precip_dt", "graupel_dt"]

############################################
#                functions
############################################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='Aggregate 1 day files to month(3h) and year (24h), while also fixing neg precip')
    parser.add_argument('model',     help='model')
    parser.add_argument('scenario',  help='scenario to process; of form sspXXX')
    parser.add_argument('part',      help='1 = 1950-2054;  2 = 2055-2099')
    parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")
    parser.add_argument('CMIP',      help='CMIP5 or CMIP6')

    return parser.parse_args()

## READ IN PCP bc files!!!
def get_icar_PCP_filelist(start_year, end_year, dt="3hr"):
    """returns a list of files (with full path) that fall within the period between start_year, end_year (strings %Y or int)"""

    # print(start_year,  end_year)
    print(f' looking for input in: {base_in}/{model}_{scen}/{dt}_pcp/icar_*_*.nc')
    files=[]
    for y in range(int(start_year), int(end_year)+1) :

        files.extend( glob.glob(f'{base_in}/{model}_{scen}/{dt}_pcp/icar_*_{y}*.nc') )

    return files


def get_icar_ref_filelist(start_year, end_year, dt="3hr"):
    """returns a list of files (with full path) that fall within the period between start_year, end_year (strings %Y or int)"""

    ## When processing scenario scen, we want the full overlap of the reference period with obs, even thought that is partly in a scenario (technically from 2015 for CMIP6?  2005 CMIP5)
    if scen[:4]=="hist" and CMIP=="CMIP6":
        scen_load="ssp370"
    elif scen[:4]=="hist" and CMIP=="CMIP5":
        scen_load="rcp45"
    else:
        scen_load=scen

    files=[]
    for y in range(int(start_year), int(end_year)+1) :
        if CMIP=="CMIP6":
            files.extend( glob.glob(f'{ref_in}/{model}_hist/{dt}/icar_*_{y}*.nc') )
            files.extend( glob.glob(f'{ref_in}/{model}_{scen_load}_2004/{dt}/icar_*_{y}*.nc') )
            files.extend( glob.glob(f'{ref_in}/{model}_{scen_load}_2049/{dt}/icar_*_{y}*.nc') )
        elif CMIP=="CMIP5":
            files.extend( glob.glob(f'{ref_in}/{model}_historical/{dt}/icar_*_{y}*.nc') )
            files.extend( glob.glob(f'{ref_in}/{model}_{scen_load}_2005_2050/{dt}/icar_*_{y}*.nc') )
            files.extend( glob.glob(f'{ref_in}/{model}_{scen_load}_2050_2100/{dt}/icar_*_{y}*.nc') )

    if len(files)==0: print("!!! No reference files found !!!")

    return files



def get_livneh_T(icar_1file):
    """get livneh data, cropped to the icar grid (defined by icar_1file)"""

    t0 = time.time()
    print("\n- - - - - - - - - -   opening livneh files - - - - - - - - - - - - ")


    ### Livneh Temperature [degC]
    tfiles = glob.glob("/glade/campaign/ral/hap/common/Livneh_met_updated/temp_and_wind/livneh_lusu_2020_temp_and_wind.2021-05-02.19[5-9]*.nc")
    tfiles.extend(glob.glob("/glade/campaign/ral/hap/common/Livneh_met_updated/temp_and_wind/livneh_lusu_2020_temp_and_wind.2021-05-02.20*.nc"))
    tfiles.sort()
    print('   ',len(tfiles), " Livneh temperature files(years)" ) # the number of files (years)
    if test:
        livneht = xr.open_mfdataset(tfiles[:10])  # speed up for testing
        print('   opening first 10 files for faster testing')
    else:
        livneht = xr.open_mfdataset(tfiles,parallel=True)

    # # # crop livneh to ICAR bounds:
    max_lat=icar_1file.lat.max().values#+buff
    min_lat=icar_1file.lat.min().values#-buff
    max_lon=icar_1file.lon.max().values#+buff
    min_lon=icar_1file.lon.min().values#-buff

    LatIndexer, LonIndexer = 'lat', 'lon'

    livneht = livneht.sel(**{LatIndexer: slice(min_lat, max_lat ),
                            LonIndexer: slice(min_lon, max_lon)})

    livneh_tmin = livneht["Tmin"] # + 273.15    #.load()
    livneh_tmax = livneht["Tmax"] # + 273.15    #.load()


    # # clean up some data; correct time dimension:
    if 'Time' in livneh_tmin.dims:
        livneh_tmin = livneh_tmin.rename( {'Time':'time'})
    if not ('time' in livneh_tmin.dims):
        print(' warning ' )

    if 'Time' in livneh_tmax.dims:
        livneh_tmax = livneh_tmax.rename( {'Time':'time'})
    if not ('time' in livneh_tmax.dims):
        print(' warning ' )

    print("   ", time.time() - t0)
    return livneh_tmin, livneh_tmax


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



def get_dsRef_ex5y(dsRef_full, fiveY_s, fiveY_e, ref_start, ref_end ):
        """ takes strings of form %Y-%M-%D, returns dsRef_full without the 5year period defined by fiveY_s to fiveY_e"""

        #### Subset reference period:   ######
        # A. If we have overlap at the start of ref period: (NB: cond fails if ref period < 5y)
        if( ( datetime.strptime(fiveY_s, '%Y-%m-%d') <= datetime.strptime(ref_start, '%Y-%m-%d') ) and
            ( datetime.strptime(fiveY_e, '%Y-%m-%d') > datetime.strptime(ref_start, '%Y-%m-%d') )
        ):
            print(f"      ref period={fiveY_e} to {ref_end} ")
            dsI_ref    =     dsRef_full.sel(time=slice(fiveY_e,ref_end))


        # B. if 5y block starts after ref_start, and finshes before ref_end (i.e. falls within ref period):
        elif( ( datetime.strptime(ref_start, '%Y-%m-%d') < datetime.strptime(fiveY_s, '%Y-%m-%d') < datetime.strptime(ref_end, '%Y-%m-%d') ) and
              ( datetime.strptime(fiveY_e, '%Y-%m-%d') < datetime.strptime(ref_end, '%Y-%m-%d') )
        ):
            print(f"      ref period={ref_start} to {fiveY_s} and {fiveY_e} to {ref_end} ")
            ref1 = dsRef_full.sel(time=slice(ref_start,fiveY_s))
            ref2 = dsRef_full.sel(time=slice(fiveY_e,ref_end))
            dsI_ref = xr.concat([ref1, ref2], dim='time')

        # C. if 5y block starts after ref_start, and finishes after ref_end
        elif( ( datetime.strptime(ref_start, '%Y-%m-%d') < datetime.strptime(fiveY_s, '%Y-%m-%d') < datetime.strptime(ref_end, '%Y-%m-%d') ) and
              ( datetime.strptime(fiveY_e, '%Y-%m-%d') > datetime.strptime(ref_end, '%Y-%m-%d') )
        ):
            print(f"      ref period= {ref_start} to {fiveY_s} ")
            dsI_ref    =     dsRef_full.sel(time=slice(ref_start,fiveY_s))
        else: # D. else
            print(f"      ref period= {ref_start} to {ref_end} ")
            dsI_ref    =     dsRef_full.sel(time=slice(ref_start,ref_end))

        return dsI_ref


def drop_vars(ds, vars_to_drop=vars_to_drop):
    """ drop unwanted variables """
    vars_to_drop2=[]
    for v in vars_to_drop:
        if v not in ds.data_vars:
            print(f"var to drop {v} not in ds.data_vars")
        else:
            vars_to_drop2.append(v)

    ds_out = ds.drop_vars(vars_to_drop2)

    return ds_out


############################################
#                Main                      #
# ###########################################
if __name__ == '__main__':
    t00 = time.time()

    # process command line
    args = process_command_line()
    model = args.model
    scen  = args.scenario
    part  = args.part
    dt    = args.dt    # timestep is either "daily" or "3hr"
    CMIP  = args.CMIP

    print('\n ########################################################')
    print( args.model, args.scenario, args.dt, ' part', args.part,  CMIP)
    print(' ########################################################', '\n')

    base_in =  f"/glade/campaign/ral/hap/bert/{CMIP}/WUS_icar_livBC2" # PCP in! #
    ref_in  = f"/glade/derecho/scratch/bkruyt/{CMIP}/WUS_icar_LivGrd2"  # referenece files (use daily)
    path_out = f"/glade/campaign/ral/hap/bert/{CMIP}/WUS_icar_livBC2"

    # base_in =  f"/glade/campaign/ral/hap/bert/{CMIP}/WUS_icar_livBC" # PCP in! #
    # ref_in  = f"/glade/derecho/scratch/bkruyt/{CMIP}/WUS_icar_LivGrd"  # referenece files (use daily)
    # path_out = f"/glade/campaign/ral/hap/bert/{CMIP}/WUS_icar_livBC"

    # create out dir if it does not exist
    if dropvars and not os.path.exists(f"{path_out}/{model}_{scen}/{dt}"):
        os.makedirs(f"{path_out}/{model}_{scen}/{dt}")
        print("Created directory " + f"{path_out}/{model}_{scen}/{dt}")
    elif dropvars and os.path.exists(f"{path_out}/{model}_{scen}/{dt}"):
        print(f" existing files in {path_out}/{model}_{scen}/{dt} will be overwritten!")
    elif not os.path.exists(f"{path_out}/{model}_{scen}/{dt}_ta2m"):
        os.makedirs(f"{path_out}/{model}_{scen}/{dt}_ta2m")
        print("Created directory " + f"{path_out}/{model}_{scen}/{dt}_ta2m")


    ##################### define periods: ######################

    if CMIP=="CMIP6": # historical until 2014-12-31
        if scen[:4]=="hist":
            time_s=['1950-01-01','1955-01-01','1960-01-01','1965-01-01','1970-01-01','1975-01-01','1980-01-01','1985-01-01','1990-01-01','1995-01-01','2000-01-01','2005-01-01','2010-01-01']
            time_f=['1954-12-31','1959-12-31','1964-12-31','1969-12-31','1974-12-31','1979-12-31','1984-12-31','1989-12-31','1994-12-31','1999-12-31','2004-12-31','2009-12-31', '2014-12-31']
        else:
            time_s=['2015-01-01','2020-01-01','2025-01-01','2030-01-01','2035-01-01','2040-01-01','2045-01-01','2050-01-01','2055-01-01','2060-01-01','2065-01-01','2070-01-01','2075-01-01','2080-01-01','2085-01-01','2090-01-01','2095-01-01']
            time_f=['2019-12-31','2024-12-31','2029-12-31','2034-12-31','2039-12-31','2044-12-31','2049-12-31','2054-12-31','2059-12-31','2064-12-31','2069-12-31','2074-12-31','2079-12-31','2084-12-31','2089-12-31','2094-12-31','2099-12-30']

    elif CMIP=="CMIP5":  # historical until 2004-12-31
        if scen[:4]=="hist":
            time_s=['1950-01-01','1955-01-01','1960-01-01','1965-01-01','1970-01-01','1975-01-01','1980-01-01','1985-01-01','1990-01-01','1995-01-01','2000-01-01']
            time_f=['1954-12-31','1959-12-31','1964-12-31','1969-12-31','1974-12-31','1979-12-31','1984-12-31','1989-12-31','1994-12-31','1999-12-31','2004-12-31']
        else:
            time_s=['2005-01-01','2010-01-01','2015-01-01','2020-01-01','2025-01-01','2030-01-01','2035-01-01','2040-01-01','2045-01-01','2050-01-01','2055-01-01','2060-01-01','2065-01-01','2070-01-01','2075-01-01','2080-01-01','2085-01-01','2090-01-01','2095-01-01']
            time_f=['2009-12-31','2014-12-31','2019-12-31','2024-12-31','2029-12-31','2034-12-31','2039-12-31','2044-12-31','2049-12-31','2054-12-31','2059-12-31','2064-12-31','2069-12-31','2074-12-31','2079-12-31','2084-12-31','2089-12-31','2094-12-31','2099-12-30']


    if int(args.part)==1:
        ts=0

    elif int(args.part)==2:  # if we've run sth before
        # exp: get index in time_s from last completed run:
        # if len(done)==0:
        if dropvars:
            done=sorted(glob.glob(f"{path_out}/{model}_{scen}/{dt}/icar_{dt}_livgrid_{model}_{scen.split('-')[0]}_*.nc" ))
        else:
            done=sorted(glob.glob(f"{path_out}/{model}_{scen}/{dt}_ta2m/icar_{dt}_livgrid_{model}_{scen.split('-')[0]}_*.nc" ))
        last_run_s=done[-1].split('_')[-1].split('-')[0]
        last_run_f=done[-1].split('_')[-1].split('-')[1]
        print(f"last succesfull 5y block was {last_run_s} to {last_run_f} ")

        res = [i for i in time_s if last_run_s in i]

        if time_s.index(res[0])==len( time_s): # if we ran to completion.
            print("done, no need to rerun. Stopping")
            sys.exit()
        elif os.path.getsize(done[-1])==os.path.getsize(done[0]):
        # elif os.path.getsize(done[-1])==os.path.getsize(done[-2]): # check if File sizes of last 2 files are similar: (if last file was written to completion)
            ts = time_s.index(res[0]) +1
            print(f"   last block ran to completion, (re)starting at {time_s[ts]}")
        elif time_s.index(res[0])==2 or time_s.index(res[0])==-1:  #(except when it is the 2nd or last file, then the file sizes are different. )
            ts = time_s.index(res[0])+1
            print(f"   check last 5y block's filesize. (re)starting at {time_s[ts]}")
        else: # redo the last file
            ts = time_s.index(res[0])
            print(f"   last block did not run to completion, (re)starting at {time_s[ts]}")

    elif int(args.part)==3:  # custom
        time_s=['2085-01-01']  # CMCC 2445
        time_f=['2089-12-31']
        # time_s=['2040-01-01']  # GFDL rcp45, CCSM 85
        # time_f=['2044-12-31']
        ts=0



    #- - - - - - -B.  define (full) reference period - - - - - - -
    ref_start = '1950-01-01'  # the range of the reference dataset that overlaps with the obs. NOTE: Different for CMIP5!
    # ref_end   = '2014-12-31'   # '2004-12-31' for CMIP5!
    ref_end   = '2018-12-31'

    print("      Memory use before loading anythng:")
    # Getting % usage of virtual_memory ( 3rd field)
    print('      * * *   RAM memory % used:', psutil.virtual_memory()[2], '   * * *   ')
    # Getting usage of virtual_memory in GB ( 4th field)
    print('      * * *   RAM Used (GB):', psutil.virtual_memory()[3]/1000000000, '   * * *   ')


    # get file list for full ref period: load 3hr, make daily.
    print(f"* * * loading Reference data... * * *")
    files_ref = get_icar_ref_filelist(ref_start.split('-')[0], ref_end.split('-')[0], dt="3hr")
    print(f"   loading {len(files_ref)} icar files: {files_ref[0].split('/')[-1]} to {files_ref[-1].split('/')[-1]} ")
    dsRef_full = xr.open_mfdataset( files_ref)

    print(f"   Ref full time: {dsRef_full.time.values.min()} to {dsRef_full.time.max().values} ")

    # # make TMin/Tmax from Ref:
    print("   making daily tmin/tmax values from 3hr ta2m Reference data")
    dsTmin_ref = dsRef_full.ta2m.resample(time='1D').min(dim='time').load()
    dsTmax_ref = dsRef_full.ta2m.resample(time='1D').max(dim='time').load()
    # dsTmin_ref = dsRef_full['Tmin'].load()  # reference data is NO LONGER daily
    # dsTmax_ref = dsRef_full['Tmax'].load()  # so make daily before loading.


    # print("      Memory use after loading daily ICAR ref:")
    # # Getting % usage of virtual_memory ( 3rd field)
    # print('      * * *   RAM memory % used:', psutil.virtual_memory()[2], '   * * *   ')
    # # Getting usage of virtual_memory in GB ( 4th field)
    # print('      * * *   RAM Used (GB):', psutil.virtual_memory()[3]/1000000000, '   * * *   ')

    # - - - - - C. Define data to match:  (Livneh) - - - - -
    icar_1_for_grid = xr.open_mfdataset( files_ref[0])  # one ICAR file to crop livneh with
    livneh_tmin, livneh_tmax = get_livneh_T(icar_1file=icar_1_for_grid.isel(time=0))

    #load the var to correct;
    livneh_tmin = livneh_tmin.load()
    livneh_tmax = livneh_tmax.load()

    # print("      Memory after loading livneh (bf starting 5y loop):")
    # # Getting % usage of virtual_memory ( 3rd field)
    # print('      * * *   RAM memory % used:', psutil.virtual_memory()[2], '   * * *   ')
    # # Getting usage of virtual_memory in GB ( 4th field)
    # print('      * * *   RAM Used (GB):', psutil.virtual_memory()[3]/1000000000, '   * * *   ')

    #################   Loop through the 5y periods:    ###############
    # for t in [13,25]:
    for t in range(ts,len(time_s)):
        t0=time.time()
        print(f"\n   ________    Processing period {time_s[t]} to {time_f[t]}: ________ ")
        start_year = time_s[t].split('-')[0]
        end_year   = time_f[t].split('-')[0]

        # ICAR input data to correct:
        files_in =  get_icar_PCP_filelist(time_s[t].split('-')[0], time_f[t].split('-')[0], dt=dt)
        dsI_in   =  xr.open_mfdataset( files_in ) # loaded into memory in bc_func

        # # subset reference data, (leave out 5years)
        dsTmin_ref_ex5y= get_dsRef_ex5y(dsTmin_ref,  fiveY_s=time_s[t], fiveY_e=time_f[t], ref_start=ref_start, ref_end=ref_end )
        dsTmax_ref_ex5y= get_dsRef_ex5y(dsTmax_ref,  fiveY_s=time_s[t], fiveY_e=time_f[t], ref_start=ref_start, ref_end=ref_end )
        print(f"   Ref time: {dsTmin_ref_ex5y.time.values.min()} to {dsTmin_ref_ex5y.time.max().values} ")


        # --------  the bias correction functions    ---------
        T_corrected_ds = correct_temperature( this_ds     = dsI_in #pcp_corrected_ds
                                             ,dsObs_tmin  = livneh_tmin
                                             ,dsObs_tmax  = livneh_tmax
                                             ,dsRef_tmin  = dsTmin_ref_ex5y
                                             ,dsRef_tmax  = dsTmax_ref_ex5y
                                             ,bc_by_month = bc_by_month
                                             )

        print("      Memory use after bc:")
        # Getting % usage of virtual_memory ( 3rd field)
        print('   * * *   RAM memory % used:', psutil.virtual_memory()[2], '   * * *   ')
        # Getting usage of virtual_memory in GB ( 4th field)
        print('   * * *   RAM Used (GB):', psutil.virtual_memory()[3]/1000000000, '   * * *   ')


        #_______ calculate relative humidity at 2m: ________
        if dt == "3hr" and 'ta2m' in T_corrected_ds.data_vars and calc_relhum:
            print("    calculating relative humidity" )
            # if static_P==True:
            #     # make a np array for the length of the input data with (static) pressure:
            #     P = np.repeat(dsP.psfc.values[np.newaxis, 12, :, :], len(ds.time), axis=0)
            # else: # if the data has surface pressure available:
            try:
                T_corrected_ds.psfc
            except AttributeError:
                print("no psfc in source dataset!")
            else:
                P = T_corrected_ds.psfc.values

            # calculate relative humidity:
            relhum = T_corrected_ds.hus2m.copy()
            relhum.values = relative_humidity(t=T_corrected_ds.ta2m.values, qv=T_corrected_ds.hus2m.values, p= P )
            # replace values over 1:
            relhum.values[relhum.values>1]=1.0
            # replace negative values
            relhum.values[relhum.values<0.0]=0.0
            ## if it is a dataArray we can add it like this:
            T_corrected_ds = T_corrected_ds.assign(relhum2m=relhum)
            T_corrected_ds.relhum2m.attrs['standard_name'] = "relative_humidity_2m"
            T_corrected_ds.relhum2m.attrs['units'] = "-"

        #  remove vars
        if dropvars==True :
            print(f"   dropping unwanted variables ")
            T_corrected_ds = drop_vars(T_corrected_ds)



        #####################   # save to file   ######################
        if dropvars:
            file_out =  f"{path_out}/{model}_{scen}/{dt}/icar_{dt}_livgrid_{model}_{scen.split('-')[0]}_{time_s[t].split('-')[0]}-{time_f[t].split('-')[0]}.nc"
        else:
            file_out =  f"{path_out}/{model}_{scen}/{dt}_ta2m/icar_{dt}_livgrid_{model}_{scen.split('-')[0]}_{time_s[t].split('-')[0]}-{time_f[t].split('-')[0]}.nc"
        T_corrected_ds.to_netcdf( file_out)

        print("      Memory use after saving to file:")
        # Getting % usage of virtual_memory ( 3rd field)
        print('   * * *   RAM memory % used:', psutil.virtual_memory()[2], '   * * *   ')
        # Getting usage of virtual_memory in GB ( 4th field)
        print('   * * *   RAM Used (GB):', psutil.virtual_memory()[3]/1000000000, '   * * *   ')

        print(" \n ")
        print("    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")
        print(f"    - - -  bias corrected file written to {file_out}   - - - ")
        print(f"    - - -  took {np.round(time.time()-t0,1)} sec   - - - " )
        print("   - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")
        print(" \n ")

    print(f"    - - - {model} {scen} {dt} {part} took {np.round((time.time()-t00)/60,1)} min   - - - " )