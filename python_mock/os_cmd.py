import os


print('h')
exec(
    open("/test/test.sh").
        read().replace("(", "(\"").replace(")", "\")"))

def my_function(src_dir):
    # test = exec(
    #     open("/test/test.sh").
    #         read().replace("(", "(\"").replace(")", "\")"))
    os.system('dir ' + src_dir)

def my_function2(src_dir):
    print('hi')
    my_function(src_dir)

if __name__ == '__main__':
    my_function(r'C:\Users\rithomas\Desktop\NokiaCemod\work')