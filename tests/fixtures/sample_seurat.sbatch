#!/bin/bash
#SBATCH --job-name=seurat_test
#SBATCH --mem=256G
#SBATCH --cpus-per-task=8
#SBATCH --time=12:00:00

module load R/4.3

Rscript -e '
library(Seurat)
obj <- readRDS(commandArgs(trailingOnly=TRUE)[1])
obj <- SCTransform(obj)
obj <- FindMarkers(obj, ident.1 = "cluster1")
saveRDS(obj, "output.rds")
'
