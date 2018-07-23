##########################################################################
# Program: add_filename.sh
# Author:  Craig Nobili
#
# Description:
#
# Displays the contents of the filename passed in and adds the filename
# to each record using the piple (|) as a delimiter.  This script is
# used as a preprocessor script for the Oracle external table interface,
# allows one to add the filename(s) in the location clause as a data
# element, i.e. as the filename is added to each record in the file.
#
##########################################################################

# Check for command line argument
if [ $# -ne 1 ]
then
  echo "Usage: add_filename.sh <filename_path>\n"
  exit 1
fi

file_full_path=$1

while read line
do
  filename=$(/bin/basename $file_full_path)
  echo "${line}|${filename}"
done < $file_full_path
