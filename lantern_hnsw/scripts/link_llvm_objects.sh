#!/bin/bash
set -euo pipefail

BC_FILES=""

if [ $# -lt 2 ]
  then
      echo "Usage: link_llvm_objects.sh 'obj1.o;obj2.o;obj3.o' /abs/path/to/build_dir"
    exit 1
fi

bitcode_dir="$2/bitcode"
bitcode_target_dir="$bitcode_dir/lantern" 
mkdir -p "$bitcode_target_dir"

for obj in ${1//;/ } ; do 
   # Get relative path from absolute, so we will get a path like `src/hnsw/hnsw.c.o`
   relative_path=${obj#"$2/CMakeFiles/lantern.dir/"}
   # Get dirname from relative path (e.g `src/hnsw`)
   dir_part=$(dirname "$relative_path")
   mkdir -p "$bitcode_target_dir/$dir_part"
   # Change suffix from .o to .bc
   obj_bc=${relative_path%.*}.bc
   obj_bc_path="$bitcode_target_dir/$obj_bc"
   cp $obj "$obj_bc_path"

   BC_FILES="$BC_FILES lantern/$obj_bc"
done

# Link bc files into lantern.index.bc
pushd $bitcode_dir >/dev/null
 llvm-lto -thinlto -thinlto-action=thinlink -o $bitcode_dir/lantern.index.bc $BC_FILES
popd >/dev/null
