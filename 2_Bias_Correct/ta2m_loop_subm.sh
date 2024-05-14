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

CMIP=CMIP6

# # # # # #    Setings     # # # # # #
dt=3hr
# dt=daily
part=2  # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth.)


if [ "$CMIP" == "CMIP5" ] ; then
    # allMods=( CCSM4 CMCC-CM CNRM-CM5 CanESM2 GFDL-CM3 MIROC5 MRI-CGCM3 )# HadGEM2-ES
    allMods=( CanESM2  ) # CCSM4 CMCC-CM CNRM-CM5 CanESM2 GFDL-CM3 MIROC5 MIROC5 CCSM4
    # allScens=( historical rcp45 rcp85 )
    allScens=( rcp45 )
elif [ "$CMIP" == "CMIP6" ] ; then
    # allMods=( CanESM5 CMCC-CM2-SR5 MIROC-ES2L NorESM2-MM ) #
    allMods=( MPI-M.MPI-ESM1-2-LR ) # ) #
    # allScens=(  ssp245  ssp370 ssp585  )
    allScens=( hist )
fi
echo "########################################################## "
echo " Submitting ta2m bias correction to Livneh grid for: "
echo "   ${allMods[*]}"
echo "   ${allScens[*]}"
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

    #PBS -l select=1:ncpus=1:mem=350GB
    #PBS -l walltime=12:00:00
    #PBS -A P48500028
    #PBS -q casper
    #PBS -N T_$model
    #PBS -o job_output/BC2LIV_CMIP5_TA2M.out
    #PBS -j oe

    module load conda
    conda activate mypy39

    echo "   launching $model $scen "

    # # #    Run the script    # # #
    mkdir -p job_auto_${CMIP}_ta2m_${dt}
    python -u BC_Icar2Liv_5y_ta2m.py $model $scen $part $dt $CMIP >& job_auto_${CMIP}_ta2m_${dt}/${model}_${scen}_${dt}


EOS

done
done


## below doesnt work somehow?
    # __conda_setup="$('/glade/work/yifanc/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    # eval "$__conda_setup"
    # unset __conda_setup
    # conda activate /glade/work/yifanc/anaconda3/envs/py3
