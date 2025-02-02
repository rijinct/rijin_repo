if [ $(id -u) -eq 0 ]; then
        #read -p "Enter username : " username
        #read -s -p "Enter password : " password
        username="dpdb"
        egrep "^$username" /etc/passwd >/dev/null
        if [ $? -eq 0 ]; then
                echo "$username user exists!"
                exit 1
        else
                #pass=$(perl -e 'print crypt($ARGV[0], "password")' $password)
                pass="Nov$2021"
                useradd -m -p "$pass" "$username"
                usermod -aG wheel $username
                [ $? -eq 0 ] && echo "User has been added to system!" || echo "Failed to add a user!"
        fi
else
        echo "Only root may add a user to the system."
        exit 2
fi


Note: Sometimes wheel group wont be present so use sudo. ie, usermod -aG sudo dpdb 