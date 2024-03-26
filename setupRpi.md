# Set-up Raspberry pi

This guide assumes you are using a Raspberry pi 3.

- Download installer from [Raspberry Pi OS](https://www.raspberrypi.com/software/)

- Set-up your own credentials, e.g.
    - hostname: energydatahub.local
    - user: pi
    - password: waterstoflab
    - Flash an SD card with latest Raspbian

- Open windows explorer
    - Navigate to the boot partition of the SD card
    - Create a file named ssh in the boot partition

- Set-up wifi connection to eduroam
    - Follow e.g. [this tutorial](https://inrg.engineering.ucsc.edu/howto-connect-raspberry-to-eduroam/)
    - Make a hash like this:
        ```console 
        ~ $ echo -n '<your_password>' | iconv -t utf-16le | openssl md4
        ```
        and add it to the network definition in '/etc/wpa_supplicant/wpa_supplicant.conf', i.e. 
        ```
        network={
        ssid="eduroam"
        scan_ssid=1
        password=hash:6d3b5....
        ...
        }
        ```
    - Create a bash script /usr/local/bin/connect_wifi.sh
        ```sh
        #!/usr/bin/env bash

        echo "Killing old processes and connecting to wifi"
        sudo killall wpa_supplicant
        sleep 5
        sudo wpa_supplicant -c/etc/wpa_supplicant/wpa_supplicant.conf -iwlan0
        ```
        Set the permissions:
        ```console
        ~ $ sudo chmod 755 /usr/local/bin/connect_wifi.sh
        ```
    - Open crontab
        ```
        ~ $ sudo crontab -e
        ```
        and add this line
        ```
        @reboot sleep 20; /usr/local/bin/connect_wifi.sh
        ```

- Have the Pi log to RAM instead of disk, by following [this guide](https://mcuoneclipse.com/2019/04/01/log2ram-extending-sd-card-lifetime-for-raspberry-pi-lorawan-gateway/)
    - Note that Log2ram ram size should be larger than /var/log
        Check with
        ```console
        ~ $ sudo du -sh /var/log
        ```

- Setup a direct ethernet connection 

    Let your Raspberry Pi acts as a mini-server directly connected to another device
    
    ```console
    ~ $ sudo nano /etc/dhcpcd.conf
    ```


    add this section:
    ```
    # Example static IP configuration:
    interface eth0
    static ip_address=192.168.0.10/24
    static ip6_address=fd51:42f8:caae:d92e::ff/64
    static routers=192.168.0.1
    static domain_name_servers=192.168.0.1 8.8.8.8 fd51:42f8:caae:d92e::1
    ```


    ```console
    ~ $ sudo systemctl restart dhcpcd
    ```

Connect the Ethernet cable: Plug one end of the cable into the Ethernet port on your Raspberry Pi and the other end into the Ethernet port of the other device.
Configure Static IP (if necessary): By default, your Raspberry Pi might try to obtain an IP address automatically (DHCP). If you want to directly connect to another device without relying on a DHCP server, you'll need to configure a static IP address on both your Raspberry Pi and the other device.
For Raspberry Pi: Edit the /etc/dhcpcd.conf file and set a static IP address, subnet mask, gateway (usually your Pi's IP), and DNS server. You can find tutorials on setting static IP for Raspberry Pi online.
For other device: The method to set a static IP varies depending on your device's operating system. Refer to your device's manual or search online for specific instructions.



14.	EXFAT stuff 
sudo apt-get install exfat-fuse exfat-utils
Rclone install:
29.	Firefox install: Apparently, you can only correctly get the google auth token if you login via firefox, needed for google drive, chromium is not supported, so install firefox:
https://linuxhint.com/install_firefox_raspberry_pi_os/
30.	Rclone install:
https://pimylifeup.com/raspberry-pi-rclone/
31.	With rclone config setup a remote to google drive
Note that waiting for firefox and google to load can take a very long time, too heavy for RPi…
Finally, Pi hangs up, so restarted and temporarily reduced GPU memory, then successful.
5.	On the command line: sudo raspi-config
a.	Interfacing options: enable VNC and camera
b.	Advanced options: expand filesystem, memorysplit: 256MB (or 312?) to GPU 
6.	Reboot 
7.	VNC to 
hostname: raspberrypi.local
login:pi


