#!/bin/bash

#########################################################################################
#########################               Settings            #############################
#########################################################################################


CMIP=CESM2
part=2  # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth.)
# dt=3hr #
dt=daily

# CMIP=CMIP5
# part=1   # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth.)
# dt=3hr
#________________________________________________________________________
# N.B. Set paths in BC_Icar2Liv_5y_pcp.py AND BC_Icar2Liv_5y_ta2m.py  !!!
#________________________________________________________________________


#-----------------    CMIP5    ------------------------
if [ "$CMIP" == "CMIP5" ] ; then
    # allMods=( CanESM2 CCSM4 CMCC-CM CNRM-CM5  GFDL-CM3 MIROC5 MRI-CGCM3 )# HadGEM2-ES
    # allMods=( CanESM2 CCSM4 CMCC-CM CNRM-CM5  MIROC5 MRI-CGCM3 ) #  GFDL-CM3
    allMods=( MIROC5 ) # CNRM-CM5 GFDL-CM3 MIROC5 MRI-CGCM3 )

    allScens=( historical ) # rcp85
    # allScens=( historical rcp45  )

#-----------------    CMIP6    ------------------------
elif [ "$CMIP" == "CMIP6" ] ; then
    # allMods=( CanESM5  )
    # allMods=( MIROC-ES2L )
    # allMods=( CMCC-CM2-SR5 )
    allMods=( MPI-M.MPI-ESM1-2-LR ) #MIROC-ES2L  CMCC-CM2-SR5 )
    # allMods=( NorESM2-MM  )
    # allMods=( MIROC-ES2L  CMCC-CM2-SR5  )
    # allScens=( ssp370 ssp245 ssp585 hist ) #
    allScens=( ssp585 )

#### CESM2 large ensemble (under development) ####
elif [[ "${CMIP}" == "CESM2" ]]; then

    #cesm2-le.010.hist-ssp370
    path_in=/glade/campaign/ral/hap/bert/CESM2/livneh/regrid_input
    path_out=/glade/campaign/ral/hap/bert/CESM2/livneh/bias_corrected

    # allMods=( cesm2-le.010.hist )
    allMods=( cesm2-le.008.hist cesm2-le.010.hist ) #cesm2-le.009.hist )
    allScens=( ssp370 )

fi



#########################################################################################
###################                  Run the script               #######################
#########################################################################################

echo "########################################################## "
echo " Submitting bias correction to Livneh grid for: "
echo "   ${allMods[*]}"
echo "   ${allScens[*]}"
echo "   part = $part "
echo "  "
echo "########################################################## "
echo "  "

for model in ${allMods[@]} ; do
    for scen in ${allScens[@]} ; do

    echo " * * *  $model  $scen  * * * "
    # ____ copy template file to a version we will modify and submit: ___
    { # try
        rsync auto_submit/template_submit_BC2liv.sh auto_submit/${model}_${scen}_submit_BC2liv.sh

    } || { # catch (in case rsync isn't available or some other error. )
        echo "   rsync error"
        cp auto_submit/template_submit_BC2liv.sh auto_submit/${model}_${scen}_submit_BC2liv.sh
    }
    echo "   template copied "


    # ___ modify model, scen , CMIP ___
    sed -i "s/__DT__/$dt/g" auto_submit/${model}_${scen}_submit_BC2liv.sh
    sed -i "s/__MODEL__/$model/g" auto_submit/${model}_${scen}_submit_BC2liv.sh
    sed -i "s/__SCEN__/$scen/g" auto_submit/${model}_${scen}_submit_BC2liv.sh
    sed -i "s/__CMIP__/$CMIP/g" auto_submit/${model}_${scen}_submit_BC2liv.sh
    sed -i "s/__part__/$part/g" auto_submit/${model}_${scen}_submit_BC2liv.sh

    # modify job name
    sed -i "s/__JOBNAME__/${model}/g" auto_submit/${model}_${scen}_submit_BC2liv.sh

    # sed -i "s/end_date = .*/end_date = \"2100-01-01 00:00:00\"/g" base_run_options.nml
    echo "   model and scen set to $model $scen $CMIP"


    # ___  submit the pbs script ___
    qsub auto_submit/${model}_${scen}_submit_BC2liv.sh

    echo "   submitted auto_submit/${model}_${scen}_submit_BC2liv.sh "
    echo " "

    done
done

# # ___ clean up the generated job scripts? -> in template_submit_BC2liv.sh
# wait 10
# for model in ${allMods[@]} ; do
#     for scen in ${allScens[@]} ; do

#         rm auto_submit/${model}_${scen}_submit_BC2liv.sh

#     done
# done