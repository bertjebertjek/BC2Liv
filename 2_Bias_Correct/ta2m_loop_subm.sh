#!/bin/bash

##############################################################################
#
# This is a loop_submit.sh for ta2m bc only
#   Its function is to launch Temperature Bias correction jobs that
#   Bias correct ICAR output to the Livneh grid.
# See ../README_BC2Liv.md for more info.
#
#
# N.B. Set paths in BC_Icar2Liv_5y_ta2m.py  !!!
#
#
# Bert Kruyt, NCAR RAL feb 2024
##############################################################################

CMIP=CMIP6  # CMIP6 CMIP5 CESM2

# # # # # #    Setings     # # # # # #
dt=3hr
# dt=daily
part=1  # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth.)


if [ "$CMIP" == "CMIP5" ] ; then
    # allMods=( CCSM4 CMCC-CM CNRM-CM5 CanESM2 GFDL-CM3 MIROC5 MRI-CGCM3 )# HadGEM2-ES
    allMods=( CCSM4 CMCC-CM CNRM-CM5 GFDL-CM3 MIROC5 MRI-CGCM3 )
    allScens=( historical rcp45 rcp85 )
    # allScens=( historical )
elif [ "$CMIP" == "CMIP6" ] ; then
    # allMods=( CanESM5 CMCC-CM2-SR5 MIROC-ES2L NorESM2-MM ) #
    # allMods=( MPI-M.MPI-ESM1-2-LR ) # ) #
    allMods=( MIROC-ES2L )
    # allScens=(  ssp245  ssp370 ssp585  )
    allScens=( ssp245 )
#### CESM2 large ensemble (under development) ####
elif [[ "${CMIP}" == "CESM2" ]]; then
    # allMods=( cesm2-le.010.hist )
    allMods=( cesm2-le.008.hist ) #cesm2-le.010.hist ) #cesm2-le.009.hist )
    allScens=( ssp370 )
fi


echo "########################################################## "
echo " Submitting ta2m bias correction to Livneh grid for: "
echo "   ${allMods[*]}"
echo "   ${allScens[*]}"
echo "   dt = ${dt}"
echo "   part = ${part}"
echo "  "
echo "########################################################## "
echo "  "


for model in ${allMods[@]} ; do
    for scen in ${allScens[@]} ; do

    echo $model $scen
    #---------------------------------------------------------------------------------------

    # (EOS: The text between the delimiting identifiers (EOS in this case) is redirected to the command.)
    #  W depend=afterany:${PBS_JOBID}

    cat <<EOS | qsub -
    #!/bin/bash

    #PBS -l select=1:ncpus=1:mem=150GB
    #PBS -l walltime=09:00:00
    #PBS -A P48500028
    #PBS -q casper
    #PBS -N T_$model
    #PBS -o job_output/BC2LIV_${CMIP}_TA2M.out
    #PBS -j oe
    #PBS -m n

    module load conda
    conda activate mypy39

    echo "   launching $model $scen "

    # # #    Run the script    # # #
    mkdir -p job_auto_${CMIP}_ta2m_${dt}
    python -u BC_Icar2Liv_5y_ta2m.py $model $scen $part $dt $CMIP >& job_auto_${CMIP}_ta2m_${dt}/${model}_${scen}_${dt}

    # # # Diagnostic (more print statements)
    # python -u BC_Icar2Liv_5y_ta2m_diagn.py $model $scen $part $dt $CMIP >& test2.out

    # # # #>& job_auto_${CMIP}_ta2m_${dt}/${model}_${scen}_${dt}
EOS

done
done


## below doesnt work somehow?
    # __conda_setup="$('/glade/work/yifanc/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    # eval "$__conda_setup"
    # unset __conda_setup
    # conda activate /glade/work/yifanc/anaconda3/envs/py3
