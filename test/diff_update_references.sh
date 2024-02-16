#!/bin/bash

tmpdir="/tmp/pytest-of-liporace/pytest-current/test_convert_inpe_to_staccurrent/"
declare -a items=(
    "AMAZONIA_1_WFI_20220811_036_018_L4.json"
    "AMAZONIA_1_WFI_20220810_033_018_L4.json"
    "CBERS_4_MUX_20170528_090_084_L2.json"
    "CBERS_4_AWFI_20170409_167_123_L4.json"
    "CBERS_4_PAN10M_20190201_180_125_L2.json"
    "CBERS_4_PAN5M_20161009_219_050_L2.json"
    "CBERS_4_PAN10M_NOGAIN.json"
    "CBERS_4A_MUX_20200808_201_137_L4.json"
    "CBERS_4A_WPM_20200730_209_139_L4.json"
)

for item in ${items[@]}; do
    echo $tmpdir$item
    diff --context=3 fixtures/ref_$item $tmpdir$item
    echo "Update?"
    select yn in "Yes" "No"; do
        case $yn in
            Yes ) cp $tmpdir$item fixtures/ref_$item; break;;
            No ) break;;
        esac
    done
done
