#!/bin/bash


##############################################################################
#
# This is a loop_submit.sh for ta2m bc only
# Its function is to launch ta2m jobs, the first dependent on the 2nd, that
# Bias correct ICAR output to the Livneh grid.
# See ../README_BC2Liv.md for more info.
#
# Bert Kruyt, NCAR RAL feb 2024
##############################################################################

CMIP=CMIP5


if [ "$CMIP" == "CMIP5" ] ; then
    # allMods=( CCSM4 CMCC-CM CNRM-CM5 CanESM2 GFDL-CM3 MIROC5 MRI-CGCM3 )# HadGEM2-ES
    allMods=( MIROC5 CCSM4 ) # CCSM4 CMCC-CM CNRM-CM5 CanESM2 GFDL-CM3 MIROC5
    allScens=( historical rcp45 rcp85 )
    # allScens=( historical rcp45 )
    # allScens=( rcp85 )
elif [ "$CMIP" == "CMIP6" ] ; then
    # allMods=( CanESM5 CMCC-blabla )
    allMods=( MIROC-ES2L ) #MPI-M.MPI-ESM1-2-LR )
    allScens=( ssp245 ssp370 ssp585 hist)
fi
echo "########################################################## "
echo " Submitting ta2m bias correction to Livneh grid for: "
echo "   ${allMods[*]}"
echo "   ${allScens[*]}"
echo "  "
echo "########################################################## "
echo "  "


# # # # # #    Setings     # # # # # #
dt=3hr
# dt=daily
part=1  # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth.)

for model in ${allMods[@]} ; do
    for scen in ${allScens[@]} ; do


#---------------------------------------------------------------------------------------
# 1. First launch a dependent job for the ta2m BC: (this will run after the pcp bc in 2.)
# (EOS: The text between the delimiting identifiers (EOS in this case) is redirected to the command.)

cat <<EOS | qsub - #W depend=afterany:${PBS_JOBID}
    #!/bin/bash

    #PBS -l select=1:ncpus=1:mem=150GB
    #PBS -l walltime=12:00:00
    #PBS -A P48500028
    #PBS -q casper
    #PBS -N T_BC2Liv5
    #PBS -o job_output/BC2LIV_CMIP5_TA2M.out
    #PBS -j oe

    ### below doesnt work somehow?
    # __conda_setup="$('/glade/work/yifanc/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    # eval "$__conda_setup"
    # unset __conda_setup
    # conda activate /glade/work/yifanc/anaconda3/envs/py3

    conda activate npl-2024a

    # # #    Run the script    # # #
    mkdir -p job_auto_${CMIP}_ta2m_${dt}
    python -u BC_Icar2Liv_5y_ta2m.py $model $scen $part $dt $CMIP >& job_auto_${CMIP}_ta2m_${dt}/${model}_${scen}_${dt}

EOS

#---------------------------------------------------------------------------------------
# 2. Launch the first part of the Bias correction; the precip bc:

# mkdir -p job_auto_${CMIP}_pcp_${dt}
# # # #    Run the pcp bc script    # # #
# python -u BC_Icar2Liv_5y_PCP.py $model $scen $part $dt $CMIP >& job_auto_${CMIP}_pcp_${dt}/${model}_${scen}_${dt}
