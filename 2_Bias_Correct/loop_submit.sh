#!/bin/bash

#########################################################################################
#########################               Settings            #############################
#########################################################################################

CMIP=CMIP6
part=3     # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth.)


#-----------------    CMIP5    ------------------------
if [ "$CMIP" == "CMIP5" ] ; then
    # allMods=( CCSM4 CMCC-CM CNRM-CM5 CanESM2 GFDL-CM3 MIROC5 MRI-CGCM3 )# HadGEM2-ES
    # allMods=( MIROC5) # CCSM4 CMCC-CM CNRM-CM5 CanESM2 GFDL-CM3 MIROC5
    # allScens=( rcp45 )
    allMods=( CCSM4 MIROC5 )
    allScens=( historical ) # rcp85 )
    # allScens=( historical rcp45 rcp85 )
    # allScens=( historical rcp45 ) # rcp85 )  #
#-----------------    CMIP6    ------------------------
elif [ "$CMIP" == "CMIP6" ] ; then
    # allMods=( CanESM5  )
    # allMods=( MIROC-ES2L )
    allMods=( CMCC-CM2-SR5 )
    # allMods=( MPI-M.MPI-ESM1-2-LR )
    # allMods=( NorESM2-MM  )
    # allScens=( hist ssp370 ssp245 ssp585 )
    allScens=( ssp585 )
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