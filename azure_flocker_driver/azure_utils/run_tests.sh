#!/bin/bash

for i in {1..100}
do
	echo "Trial $i" 
	trial test_disk_manager.py
done

