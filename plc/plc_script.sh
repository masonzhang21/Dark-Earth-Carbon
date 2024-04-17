#!/bin/bash

pip install pandas firebase-admin
systemctl daemon-reload
# Infinite loop to run the script every 30 seconds
while true; do
    # 1) Run the remount script
    mount -o loop /usbstore.img /plc/usb_mount
  
  # 2) Check for the file in the mount directory and move it if it meets criteria
  # Note: Adjust '~/mount_point' to the actual mount point path
  # and ensure the pattern '??????????001.csv' matches your file naming convention
  for file in /plc/mount_point/??????????001.csv; do
    # Check if the file exists to prevent the error with null files
    if [ -e "$file" ]; then
      # Calculate file age in seconds
      file_age=$(($(date +%s) - $(stat -c %Y "$file")))
      if [ $file_age -gt 5 ]; then
        # Move and rename the file to 'plc_data.py' in the desired directory
        # Note: Change '/destination/directory' to where you want to move the file
        mv "$file" /plc/plc_data.csv
        # 3) Run python plc_upload.py (ensure you're in the correct directory or provide the full path)
        python /plc/plc_upload.py
        # 4) Delete plc_data.py after running the upload script
        rm /plc/plc_data.py
      fi
    fi
  umount /plc/usb_mount
  done
  # Wait for 30 seconds before the next iteration
  sleep 20
done
