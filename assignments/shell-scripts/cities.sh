#!/bin/sh
touch cities.txt
echo "Enter cities, and end with ctrl+D"
cat >> cities.txt
cat cities.txt | sed 's/New/Old/gi' > old-cities.txt
