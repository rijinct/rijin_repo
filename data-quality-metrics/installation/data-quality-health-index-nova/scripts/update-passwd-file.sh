P_UID=`id -u`
P_GID=`id -g`

echo "Setting user and group ids"
echo "ngdb:x:$P_UID:$P_GID::/home/ngdb:/bin/bash" >> /etc/passwd
echo "ninstall:x:$P_GID:" >> /etc/group
echo "User and group id has been set"
