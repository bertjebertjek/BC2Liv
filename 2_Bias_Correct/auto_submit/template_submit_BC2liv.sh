#!/bin/bash
#PBS -l select=1:ncpus=1:mem=150GB
#PBS -l walltime=12:00:00
#PBS -A P48500028
#PBS -q casper
#PBS -N p___JOBNAME__
#PBS -o job_output/BC2LIV_CMIP_PCP.out
#PBS -j oe
#PBS -m n

##############################################################################
#
# This is a template, modified and launched by ../loop_submit.sh
# Its function is to launch 2 jobs, the first dependent on the 2nd, that
# Bias correct ICAR output to the Livneh grid.
# See ../README_BC2Liv.md for more info.
#
# Bert Kruyt, NCAR RAL feb 2024
##############################################################################

__conda_setup="$('/glade/work/yifanc/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
eval "$__conda_setup"
unset __conda_setup
conda activate /glade/work/yifanc/anaconda3/envs/py3

# module load conda
# conda activate py311

# # # # # #    Setings     # # # # # #

# _______ SET BY BASH script ______
dt=__DT__
model=__MODEL__
scen=__SCEN__
CMIP=__CMIP__
part=__part__  # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth.)


#---------------------------------------------------------------------------------------
# 1. First launch a dependent job for the ta2m BC: (this will run after the pcp bc in 2.)
# (EOS: The text between the delimiting identifiers (EOS in this case) is redirected to the command.)

cat <<EOS | qsub -W depend=afterany:${PBS_JOBID}
    #!/bin/bash

    #PBS -l select=1:ncpus=1:mem=150GB
    #PBS -l walltime=12:00:00
    #PBS -A P48500028
    #PBS -q casper
    #PBS -N t___JOBNAME__
    #PBS -o job_output/BC2LIV_CMIP_TA2M.out
    #PBS -j oe
    #PBS -m n


    module load conda
    conda activate mypy39

    # # #    Run the script    # # #
    mkdir -p job_auto_${CMIP}_ta2m_${dt}
    python -u BC_Icar2Liv_5y_ta2m.py $model $scen $part $dt $CMIP >& job_auto_${CMIP}_ta2m_${dt}/${model}_${scen}_${dt}

    # # # #    clean up the generated job scripts   # # #
    # wait 10
    # rm auto_submit/${model}_${scen}_submit_BC2liv.sh


EOS

# ---------------------------------------------------------------------------------------
# 2. Launch the first part of the Bias correction; the precip bc:

mkdir -p job_auto_${CMIP}_pcp_${dt}
# # #    Run the pcp bc script    # # #
python -u BC_Icar2Liv_5y_pcp.py $model $scen $part $dt $CMIP >& job_auto_${CMIP}_pcp_${dt}/${model}_${scen}_${dt}




