import datetime, pip, platform, sys
start_time = datetime.datetime.now()

packageList = ['os', 'progress', 'requests', 'urllib3', 'json', 'sys', 'datetime', 'pympler', 'pandas']
pkgs = "\t" + '\n\t'.join(packageList)
python_context = "Using Python version: {}, found at: \"{}\"".format(platform.python_version(), sys.executable)
pip_context = "Using Pip version: {}".format(pip.__version__)

print(python_context)
print(pip_context)
print("Verifying availability of the required python packages:\n{}".format(pkgs))
exit(1)
for package in packageList:
    try:
        __import__(package)
        print("Found required package: {}".format(package))
    except ImportError:
        print("ALERT: Missing required package: {}. Will now attempt to ")
        pip.main(['install', package])




print("Total execution time: {}".format(datetime.datetime.now() - start_time))
