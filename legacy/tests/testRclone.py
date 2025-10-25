import subprocess

output_file_path = r'/home/pi/tmp'
remote_storage_path = r'gdrive:/data'

    
if remote_storage_path is not None:
    try:
        subprocess.run(['rclone', 'copy', output_file_path, remote_storage_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(str(e))