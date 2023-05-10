import subprocess

file_loc = r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\data\test\temp.csv"

cmd = "sed -i \"1i 1,2,3,4,5,6,7\" \"{f}\"".format(f=file_loc)
print(cmd)
subprocess.call(cmd)