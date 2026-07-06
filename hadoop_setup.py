import os
import urllib.request

def download_file(url, output_path):
    print(f"Downloading {url} to {output_path}...")
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        # Download
        urllib.request.urlretrieve(url, output_path)
        print("Success!")
    except Exception as e:
        print(f"Failed to download: {e}")

def main():
    hadoop_dir = os.path.abspath(os.path.join(os.getcwd(), "hadoop"))
    bin_dir = os.path.join(hadoop_dir, "bin")
    
    # We will use Hadoop 3.3.0 binaries
    winutils_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe"
    hadoop_dll_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/hadoop.dll"
    
    download_file(winutils_url, os.path.join(bin_dir, "winutils.exe"))
    download_file(hadoop_dll_url, os.path.join(bin_dir, "hadoop.dll"))
    
    print("\nLocal Hadoop Setup Completed!")
    print(f"HADOOP_HOME should be configured as: {hadoop_dir}")

if __name__ == "__main__":
    main()
