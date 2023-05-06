import os

def read_text_file(file_path):
    with open(file_path, 'r') as f:
        return f.readlines()

def write_text_file(file, file_content):
    with open(file, 'w') as f:
        return f.writelines(file_content)


def replace_file_content(file, old_val, new_val):
    file_content = read_text_file(file)
    new_contet = []
    for line in file_content:
            replacement = line.replace(old_val,new_val)
            new_contet.append(replacement)
    print(new_contet)
    new_file = '{}_new'.format(file)
    write_text_file(new_file, new_contet)


def execute_flowfile_replace(path,old_file,new_file):
    os.chdir(path)
    for file in os.listdir():
        print(file)
        file_path = '{path}/{file}'.format(path=path, file=file)
        replace_file_content(file_path, old_file,
                             new_file)


if __name__ == "__main__":

    path = "/Users/rthomas/Desktop/Rijin/rijin_git/python_utils/input"
    execute_flowfile_replace(path,'map_OR_child_clinic_sampling-mapping','new')