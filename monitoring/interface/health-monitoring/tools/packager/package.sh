#!/bin/bash

directory=$1
template=$2
modules=`./get_modules.py -p $directory`
sed "s:#DIR#:$modules:g" $template > __out__.spec
pyinstaller __out__.spec --clean
staticx ./dist/__main__ ./__main__