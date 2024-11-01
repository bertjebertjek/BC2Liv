

import xarray as xr
import numpy as np 
import pandas as pd
import glob, os
# import matplotlib.pyplot as plt
# import seaborn as sns
import itertools
import multiprocessing


def check_mod_scen( model, scen):

    files  = sorted(glob.glob( f"/glade/derecho/scratch/bkruyt/CMIP6/WUS_icar_LivGrd3/{model}_{scen}/3hr/icar_3hr_livgrd_*"))

    for i in range(len(files)):
        f = files[i:i+2]  # open 2 files so we catch missing data at the end of a month
        
        ds5 = xr.open_mfdataset( f )
    
        idx = np.where(ds5.time.diff(dim='time').values > ds5.time.diff(dim='time').min().values )
        
        if len(idx[0])==1:
            print( #f.split('/')[-1], idx[0],
                  model, scen, ds5.time.values[ idx[0][0] ] 
                 )
        elif len(idx[0])>1:
            for l in len(idx[0]):
                print( 
                      model, scen, ds5.time.values[ idx[0][l] ] 
                     )

#######################
######   MAIN    ######
#######################

if __name__=="__main__":

    models = ["CanESM5", "CMCC-CM2-SR5", "MIROC-ES2L", "MPI-M.MPI-ESM1-2-LR", "NorESM2-MM"] 
    scenarios = ["ssp245_2004", "ssp245_2049", "ssp370_2004", "ssp370_2049", "ssp585_2004", "ssp585_2049"]
    nprocs = len(models)*len(scenarios)  # 30

    
    # # Iterate over all combinations of list1 and list2
    # for combination in itertools.product(models, scenarios):
    #     print(combination[0], combination[1], combination)
    
    
    with multiprocessing.Pool(processes=nprocs) as pool:
        results = pool.starmap(check_mod_scen, list(itertools.product(models, scenarios)))