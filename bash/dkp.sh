#!/bin/bash

xls_path="${XL_IDP_PATH_DKP}/dkp"

done_path="${xls_path}"/done
if [ ! -d "$done_path" ]; then
  mkdir "${done_path}"
fi

json_path="${xls_path}"/json
if [ ! -d "$json_path" ]; then
  mkdir "${json_path}"
fi

find "${xls_path}" -maxdepth 1 -type f \( -name "*.xls*" \) ! -newermt '30 seconds ago' -print0 | while read -d $'\0' file
do

  if [[ "${file}" == *"error_"* ]];
  then
    continue
  fi

  echo "Will convert XLS* '${file}' to JSON '${json_path}'"
  python3 "${XL_IDP_ROOT_DKP}/scripts/dkp.py" "${file}" "${json_path}"

  if [ $? -eq 0 ]
    then
      mv "${file}" "${done_path}"
    else
      echo "ERROR during convertion ${file} to json!"
      mv "${file}" "${xls_path}/error_$(basename "${file}")"
      continue
    fi

done