. ~/.bash_profile

if [ $1 -eq 1 ] 
then
	echo "Installation is started"
elif [ $1 -ge 2 ] 
then
	echo "Pre-upgrade is started"
fi
