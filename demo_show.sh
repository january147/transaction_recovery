#!/bin/bash
# Date: Mon Nov 18 07:51:54 2019
# Author: January

if [ "$1" = "-acc" ];then
    echo '########################system init###########################'
    ./recovery.py -init
    echo '#####disk_data######'
    xxd -l 100 disk.img
    echo '######log######'
    sqlite3 data.log
    echo '##########################do transaction###########################'
    ./recovery.py -accomplish
    echo '#####disk_data######'
    xxd -l 100 disk.img
    echo '######log######'
    sqlite3 data.log
    echo '##############################recovery#############################'
    ./recovery.py -rec
    echo '#####disk_data######'
    xxd -l 100 disk.img
    echo '######log######'
    sqlite3 data.log
fi

if [ "$1" = "-abort" ];then
    echo '########################system init###########################'
    ./recovery.py -init
    echo '#####disk_data######'
    xxd -l 100 disk.img
    echo '######log######'
    sqlite3 data.log
    echo '##########################do transaction###########################'
    ./recovery.py -abort
    echo '#####disk_data######'
    xxd -l 100 disk.img
    echo '######log######'
    sqlite3 data.log
    echo '##############################recovery#############################'
    ./recovery.py -rec
    echo '#####disk_data######'
    xxd -l 100 disk.img
    echo '######log######'
    sqlite3 data.log
fi