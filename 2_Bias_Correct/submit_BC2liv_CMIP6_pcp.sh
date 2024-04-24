#!/bin/bash
#PBS -l select=1:ncpus=1:mem=200GB
#PBS -l walltime=02:00:00
#PBS -A P48500028
#PBS -q casper
#PBS -N pBC2Liv_6
#PBS -o job_output/BC2LIV_CMIP6_PCP.out
#PBS -j oe


__conda_setup="$('/glade/work/yifanc/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
eval "$__conda_setup"
unset __conda_setup
conda activate /glade/work/yifanc/anaconda3/envs/py3

# # test
# module load conda
# conda activate npl-2024a #


part=1  # part 1 = from start; part 2 = look for last output file and restart there : 3=custom (in case we need to rerun sth. Set dates in .py file)
# dt=daily
dt=3hr
CMIP=CMIP6

# # # #    Set model and scen    # # # # # #
if [[ "${CMIP}" == "CMIP5" ]]; then
    model=CanESM2
    # allScens=( hist  )
    allScens=( hist rcp45 rcp85 ) #
elif [[ "${CMIP}" == "CMIP6" ]]; then
    # model=CanESM5
    # model=CMCC-CM2-SR5
    model=NorESM2-MM
    # model=MIROC-ES2L
    # model=MPI-M.MPI-ESM1-2-LR
    # allScens=( hist ssp585 ssp370 ssp245 ) #
    allScens=( hist )
    # allScens=( ssp245 ssp370 ssp585 hist)
fi


for scen in ${allScens[@]}; do

# mkdir -p job_output_${CMIP}_pcp_${dt}/BC_5y_${model}_${scen}_${dt}_${PBS_JOBID::7} # make log dir if it does not exist

# # # #    Run the script    # # #

# python -u BC_Icar2Liv_5y_pcp.py $model $scen $part $dt $CMIP >& job_output_${CMIP}_pcp_${dt}/BC_5y_${model}_${scen}_${dt}_${PBS_JOBID::7}

# # test no Noise added:
python -u BC_Icar2Liv_5y_pcp_noNoise.py $model $scen $part $dt $CMIP >& job_output_${CMIP}_pcp_${dt}/BC_5y_${model}_${scen}_${dt}_${PBS_JOBID::7}

done
