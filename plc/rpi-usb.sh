#!/bin/bash
# Set up a Raspberry Pi 4 as a USB-C Ethernet Gadget
# Based on:
#     - https://github.com/kmpm/rpi-usb-gadget/tree/master
#     - https://raw.githubusercontent.com/thagrol/Guides/main/mass-storage-gadget.pdf

USBFILE=/usr/local/sbin/usb-gadget.sh
UNITFILE=/lib/systemd/system/usb-gadget.service

teeconfirm() {
    line=$1
    f=$2
    if ! $(grep -q "$line" $f); then
        echo
        echo "Add the line '$line' to '$f'"
        ! confirm && exit
        echo "$line" | sudo tee -a $f
    fi
}

##### Actual work #####

teeconfirm "libcomposite" "/etc/modules"

# create script to setup usb functionality
if sudo test ! -e "$USBFILE" ; then
    cat << 'EOF' | sudo tee $USBFILE > /dev/null
#!/bin/bash

gadget=/sys/kernel/config/usb_gadget/pi4

mkdir -p ${gadget}
echo 0x1d6b > ${gadget}/idVendor # Linux Foundation
echo 0x0104 > ${gadget}/idProduct # Multifunction composite gadget
echo 0x0100 > ${gadget}/bcdDevice # v1.0.0
echo 0x0200 > ${gadget}/bcdUSB # USB 2.0
echo 0xEF > ${gadget}/bDeviceClass
echo 0x02 > ${gadget}/bDeviceSubClass
echo 0x01 > ${gadget}/bDeviceProtocol

mkdir -p ${gadget}/strings/0x409
echo  "Mason" ${gadget}/strings/0x409/manufacturer
echo  "PLC USB Drive" > ${gadget}/strings/0x409/product
echo "01234567890" > ${gadget}/strings/0x409/serialnumber

mkdir ${gadget}/configs/c.1
echo 250 > ${gadget}/configs/c.1/MaxPower

mkdir -p ${gadget}/configs/c.1/strings/0x409
echo "Config 1: Mass Storage" > ${gadget}/configs/c.1/strings/0x409/configuration

mkdir -p ${gadget}/functions/mass_storage.usb0
echo 0 > ${gadget}/functions/mass_storage.usb0/lun.0/cdrom
echo 0 > ${gadget}/functions/mass_storage.usb0/lun.0/ro
echo "/usbstore.img" > ${gadget}/functions/mass_storage.usb0/lun.0/file
ln -s ${gadget}/functions/mass_storage.usb0 ${gadget}/configs/c.1/

ls /sys/class/udc > ${gadget}/UDC

udevadm settle -t 5 || :
EOF

    sudo chmod 750 $USBFILE
    echo "Created $USBFILE"
fi


# make sure $USBFILE runs on every boot using $UNITFILE
if [[ ! -e $UNITFILE ]] ; then
    cat << EOF | sudo tee $UNITFILE > /dev/null
[Unit]
Description=USB gadget initialization
After=network-online.target
Wants=network-online.target
#After=systemd-modules-load.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=$USBFILE

[Install]
WantedBy=sysinit.target

EOF
    echo "Created $UNITFILE"
    sudo systemctl daemon-reload
    sudo systemctl enable usb-gadget
fi

cat << EOF


Done setting up as USB gadget
You must reboot for changes to take effect.

If you want to disable the usb0/gadget interface then
please run 'sudo systemctl disable usb-gadget'
and reboot.

EOF
