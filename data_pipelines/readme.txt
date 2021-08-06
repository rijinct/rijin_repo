Troubleshooting steps: 

Issues during securing NiFi: While setting up nifi, you need to use the tool kit to generate the property files and the certificate files. Steps I followed for Mac are as follows: 

        1. sh tls-toolkit.sh standalone -n "localhost" -C "CN=RIJIN, OU=NIFI"

        2. cp -rv localhost/* /Users/rthomas/Desktop/Rijin/Telethon/4_Telethon_software/nifi-1.14.0/conf/

        3. you need to modify authorized-users.xmls as below: 


            <userGroupProvider>
                <identifier>file-user-group-provider</identifier>
                <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
                <property name="Users File">./conf/users.xml</property>
                <property name="Legacy Authorized Users File"></property>

                <property name="Initial User Identity 1">CN=RIJIN, OU=NIFI</property>
            </userGroupProvider>


            <authorizer>
                <identifier>file-provider</identifier>
                <class>org.apache.nifi.authorization.FileAuthorizer</class>
                <property name="Authorizations File">./conf/authorizations.xml</property>
                <property name="Users File">./conf/users.xml</property>
                <property name="Initial Admin Identity">CN=RIJIN, OU=NIFI</property>
                <property name="Legacy Authorized Users File"></property>

                <!--property name="Node Identity 1"></property-->
            </authorizer>

        3. If you want to add multiple users or if user tab in the UI is missing, then do the following: 


            In nifi.properties, Replace nifi.security.user.authorizer=single-user -authorizer with -> nifi.security.user.authorizer=managed-authorizer


        4. Before restart NIFI, delete temperory files like below: 

            rm -rf flow.xml.gz users.xml authorizations.xml

        5. Restart NIFI

        6. Import the certificate on the chrome, SSL (go to preferances -> search certificates and then add the certificate eg, CN=RIJIN_OU=NIFI.p12)



        
    