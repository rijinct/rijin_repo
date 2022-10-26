---
layout: post
title: The Power Of NiFi
subtitle: Securing NiFi
comments: true
cover-img: /assets/img/path.jpg
thumbnail-img: /assets/img/nifi-img.png
share-img: /assets/img/path.jpg
tags: [books, concepts]
---

Apache NiFi is an integrated data logistics platform for automating the movement of data between disparate systems. It provides real-time control that makes it easy to manage the movement of data between any source and any destination. It is data source agnostic, supporting disparate and distributed sources of differing formats, schemas, protocols, speeds and sizes such as machines, geo location devices, click streams, files, social feeds, log files and videos and more.

## NiFi Demo

![NiFi1](/assets/img/NiFi1.png)

![NiFi2](/assets/img/NiFi2.png)

![NiFi3](/assets/img/NiFi3.png)

![NiFi4](/assets/img/NiFi4.png)


## Complete NiFi Usecase Explanation


1. A get API request will fetch all the records from Source DataSilo in csv format
2. Convert those csv to json because we plan to store as individual json in MarkLogic. 
3. Split the json into individual flow files, so that finally we store as independent json docs
4. Manupulate the data content in the json doc to do some data check like, the field opn and ouuid should always be numeric, find the ones which are non-numeric. 

![usecase1](/assets/img/usecase1.png)

![usecase2](/assets/img/usecase2.png)

![usecase3](/assets/img/usecase3.png)


## Processor Properties

**1. ReplaceText Processor1:**

![processor1](/assets/img/processor1.png)

**2. ReplaceText Processor2:**

![processor2](/assets/img/processor2.png)

**3. ReplaceText Processor3:**

![processor3](/assets/img/processor3.png)

**4. UpdateAttribute Processor:**

![processor4](/assets/img/processor4.png)

**5. EvaluateJsonPath Processor:**

![processor5](/assets/img/processor5.png)

**6. RouteOnAttribute Processor:**

![processor6](/assets/img/processor6.png)



## NiFi Security

**NIFI Authentication:**

NiFi supports Authentication and Authorization only if SSL is enabled.

NiFi supports below Authentication strategies

1. LDAP
2. Kerberos

NiFi supports following Authorization:

1. File-based policies (managed within NiFi)
2. Ranger-based policies (managed within Ranger)
3. Custom pluggable authorizer


**Configuring LDAP to Nifi: :**

Make the below changes in nifi.properties, authorizers.xml and login-identity-providers.xml 

In login-identity-providers.xml, uncomment and update the ldap-provider section as below: 
~~~
<provider>
<identifier>ldap-provider</identifier>
<class>org.apache.nifi.ldap.LdapProvider</class>
<property name="Authentication Strategy">SIMPLE</property>

<property name="Manager DN">CN=Rijin Thomas,OU=Researchers,OU=Users,OU=Company,DC=icd,DC=uw,DC=edu,DC=au</property>
<property name="Manager Password">******</property>

<property name="TLS - Keystore"></property>
<property name="TLS - Keystore Password"></property>
<property name="TLS - Keystore Type"></property>
<property name="TLS - Truststore"></property>
<property name="TLS - Truststore Password"></property>
<property name="TLS - Truststore Type"></property>
<property name="TLS - Client Auth"></property>
<property name="TLS - Protocol"></property>
<property name="TLS - Shutdown Gracefully"></property>

<property name="Referral Strategy">FOLLOW</property>
<property name="Connect Timeout">10 secs</property>
<property name="Read Timeout">10 secs</property>

<property name="Url">ldap://hodc01.ichr.uwa.edu.au</property>
<property name="User Search Base">OU=Company,DC=icd,DC=uw,DC=edu,DC=au</property>
<property name="User Search Filter">cn={0}</property>

<property name="Identity Strategy">USE_USERNAME</property>
<property name="Authentication Expiration">12 hours</property>
</provider>
}
foo(3)
~~~

In nifi.properties file, make the following changes:

~~~

         nifi.login.identity.provider.configuration.file=./conf/login-identity-providers.xml
         nifi.security.user.login.identity.provider=ldap-provider
~~~


And here is the same code yet again but with line numbers:

{% highlight javascript linenos %}
var foo = function(x) {
  return(x + 5);
}
foo(3)
{% endhighlight %}

## Boxes
You can add notification, warning and error boxes like this:

### Notification

{: .box-note}
**Note:** This is a notification box.

### Warning

{: .box-warning}
**Warning:** This is a warning box.

### Error

{: .box-error}
**Error:** This is an error box.
