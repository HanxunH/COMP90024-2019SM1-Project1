#!/bin/bash
# Created by the University of Melbourne job script generator for SLURM
# Tue Mar 26 2019 20:54:17 GMT+1100 (澳大利亚东部夏令时间)

# Partition for the job:
#SBATCH --partition=physical

# The name of the job:
#SBATCH --job-name="xuwang2/8core"

# The project ID which this job should run under:
#SBATCH --account="COMP90024"

# Maximum number of tasks/CPU cores used by the job:
#SBATCH --ntasks=8

# The amount of memory in megabytes per process in the job:
#SBATCH --mem-per-cpu=20480

# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
# ends successfully
#SBATCH --mail-type=END

# Use this email address:
#SBATCH --mail-user=xuwang2@student.unimelb.edu.au

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=0-1:0:00

# check that the script is launched with sbatch
if [ "x$SLURM_JOB_ID" == "x" ]; then
   echo "You need to submit your job to the queuing system with sbatch"
   exit 1
fi

# Run the job from this directory:
cd /home/xuwang2/COMP90024-2019SM1-Project1/
# The modules to load:
module load MPICH/3.2.1-GCC-6.2.0
module load Python/3.5.2-GCC-6.2.0

# The job command(s):
mpirun -n 8 python3 main.py
