Everything is in C:\Users\rithomas\Desktop\NokiaCemod\4_Cemod_software\JetBrains\IntelliJ IDEA Community Edition 2020.3.1

1) To create a new project or workspace, create a empty projet and add modules

2) to change or add inspection setting to point syntax errors and all -> file->settings->inspections

3) Added a lot of short cuts in file->settings->keymap 
	i) to search for a field and able to do ctrl-k and ctrl+shift+k, for that added double click in find caret, then added ctrl-K and ctrl shift K for moving the find up n down
	
4) When u want to run shell script, in edit configuration set the interpretter as C:\Program Files\Git\bin\bash.exe

xxxx --> To properly compile the code on intelliJ, the below steps I followed. 
	1) All dependent jars are created in m2 by runnin maven build, say for scheduler, I had to compile ETL and SDK
	2) File-> Open -> create from pom-cloudera.xml-> create new project	
	3) Step 2 will create it, but go to file-> proj_structure and remove all the non-CD jars, keep only CD jars. 
	4) Noticed, the jars coming back eachj time I deleted. so, after deleting continuously 5 times it got removed
	
	
5) To word Wrap - Ctrl + Alt + L

6) Remember, when you put __init__.py, the package starts from there. Eg, I have a structure like "rijin/data_quality/com/nokia/dqhi". Now if I want my code to have root package as com, then add init.py from com to all the subdirectories. Now, to compile code with com in intellij, you need to open intellij workspace with one folder above com ie, data_quality. 
if you open workspace from rijin, then intellij will by default consider rijin as root. But, linux box will always consider root based on the init.py. ie, if init.py is present in com and subdirectories, then com is the root. If init.py is present in rijin, then it will take root as rijin and then u need to change everything as from rijin.data_quality.com etc
	
	
	
	
	
7. #1: Had an issue with Intellij, so to debub it, ran it from command promt to see the error
		cd /Applications/IntelliJ IDEA.app/Contents/MacOS/
		./idea
	
	#2: Fixed the issue by removing a plug-in 

		/Users/rthomas/Library/Application Support/JetBrains/IdeaIC2021.2/plugins
		removed the plugin