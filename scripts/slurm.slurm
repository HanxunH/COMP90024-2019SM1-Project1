#!/bin/bash

#SBATCH --partition=cloud
#SBATCH --ntasks=8
#SBATCH --job-name="xuwang2_big"
#SBATCH --account="COMP90024"
#SBATCH --mem-per-cpu=20480
#SBATCH --mail-type=FAIL
#SBATCH --mail-type=END
#SBATCH --mail-user=xuwang2@student.unimelb.edu.au
#SBATCH --time=1-0:0:00

# check that the script is launched with sbatch
if [ "x$SLURM_JOB_ID" == "x" ]; then
   echo "You need to submit your job to the queuing system with sbatch"
   exit 1
fi

# Run the job from this directory:
cd /home/$USER/COMP90024-2019SM1-Project1/
# The modules to load:
module load MPICH/3.2.1-GCC-6.2.0
module load Python/3.5.2-GCC-6.2.0

# The job command(s):
mpirun -n 8 python3 main.py --grid_file_path "../data/melbGrid.json" --twitter_data_file_path="../data/bigTwitter.json"

