<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" version="1.0" encoding="UTF-8"
        indent="yes" />


    <xsl:template match="/">
        <stc>
            <xsl:apply-templates />
        </stc>
    </xsl:template>
    <xsl:key name="kElsByGroup" match="row" use="tablename" />
	<xsl:key name="kElsByGroupCol" match="row" use="concat(tablename, '|', columnname)"/>
	<xsl:key name="kElsByGroupReadByList" match="row" use="concat(tablename, '|', readbylist)"/>
	<xsl:key name="kElsByGroupWrittenByList" match="row" use="concat(tablename, '|', writtenbylist)"/>
	<xsl:key name="kElsByGroupTransformList" match="row" use="concat(tablename, '|',columnname,'|',writtenbylist, '|', formula)"/>	
	<xsl:key name="kElsByGroupExecList" match="row" use="concat(tablename, '|', jobidlist)"/>	
	<xsl:key name="kElsByGroupUltimateSource" match="row" use="concat(tablename, '|', columnname,'|',writtenbylist)"/>	
    <xsl:template match="row">
        <xsl:apply-templates />
    </xsl:template>
    <xsl:template
        match="row[generate-id()=generate-id(key('kElsByGroup',tablename)[1])]">
        <dataBase>
            <dataBaseBasicInfo>
                <name>CEMOD</name>
                <type>HDFS</type>
            </dataBaseBasicInfo>
            <schemaList>
                <schema>
                    <name>CEMOD</name>
                    <tableList>
                        <table>
                            <physicalTableName>
                                <tableName>
                                    <xsl:value-of select="tablename"></xsl:value-of>
                                </tableName>
                                <application>Event</application>
                                <level>
                                    <xsl:value-of select="level"></xsl:value-of>
                                </level>
                                <server>CEM</server>
                                <system>Customer Experience Management</system>
                                <description>
                                    <xsl:value-of select="table_description"></xsl:value-of>
                                </description>
                                <sourcepath>CUSTOMER_ANALYSIS</sourcepath>
                                <interval>
                                    <xsl:value-of select="interval"></xsl:value-of>
                                </interval>
                            </physicalTableName>
                            <!-- Table name="{tablename}" -->
                            <columnList>
                                <xsl:for-each
                                    select="key('kElsByGroup', tablename)[generate-id() = generate-id(key('kElsByGroupCol', concat(tablename, '|', columnname))[1])]">
                                    <xsl:element name="column">
                                        <name>
                                            <xsl:value-of
                                                select="columnname"></xsl:value-of>
                                        </name>
                                        <nativeType>
                                            <xsl:value-of
                                                select="datatype"></xsl:value-of>
                                        </nativeType>
                                        <description>
                                            <xsl:value-of
                                                select="definition"></xsl:value-of>
                                        </description>
                                    </xsl:element>
                                </xsl:for-each>
                            </columnList>
                            <!-- keyList -->
                            <keyList>
                                <xsl:for-each
                                    select="key('kElsByGroup',tablename)[1]">
                                    <xsl:if test="keyName">
                                        <xsl:element name="key">
                                            <keyName>
                                                <xsl:value-of
                                                    select="keyName"></xsl:value-of>
                                            </keyName>
                                            <keyType>
                                                <xsl:value-of
                                                    select="keyType"></xsl:value-of>
                                            </keyType>
                                        </xsl:element>
                                    </xsl:if>
                                </xsl:for-each>
                            </keyList>
                            <!-- readByList -->
                            <readByList>
                                <xsl:for-each
                                    select="key('kElsByGroup', tablename)[generate-id() = generate-id(key('kElsByGroupReadByList', concat(tablename, '|', readbylist))[1])]">
                                    <xsl:if test="readbylist!=''">  
                                        <xsl:element name="execuTable">
                                            <application>
                                                <xsl:value-of
                                                    select="readbylist"></xsl:value-of>
                                            </application>
                                            <description>
                                                         <xsl:value-of select="readbytabledescription"></xsl:value-of>
                                            </description>
                                        </xsl:element>
                                     </xsl:if>
                                </xsl:for-each>
                            </readByList>
                            <!-- writtenByList -->
                            <writtenByList>
                                    <xsl:for-each
                                        select="key('kElsByGroup', tablename)[generate-id() = generate-id(key('kElsByGroupWrittenByList', concat(tablename, '|', writtenbylist))[1])]">
                                        <xsl:if test="writtenbylist!=''">
                                        <xsl:element name="execuTable">
                                                     <system>
                                                        <xsl:value-of
                                                            select="writtenbylist"></xsl:value-of>
                                                     </system>
                                                     <description> 
                                                        <xsl:value-of select="writtenbytabledescription"></xsl:value-of>
                                                     </description>
                                         </xsl:element>
                                         </xsl:if>
                                     </xsl:for-each>    

                            </writtenByList>
                            <!-- ultimatesourcelist -->
                            <ultimateSourceList>
                                <xsl:for-each
                                  select="key('kElsByGroup', tablename)[generate-id() = generate-id(key('kElsByGroupTransformList', concat(tablename, '|', columnname,'|',writtenbylist, '|', formula))[1])]">
                                    <xsl:element name="column">
                                        <columnName>
                                            <xsl:value-of
                                                select="columnname"></xsl:value-of>
                                        </columnName>
                                        <ultimateSourceDatabase>CEMOD</ultimateSourceDatabase>
                                        <ultimateSourceTable>
                                            <xsl:value-of
                                                select="writtenbylist"></xsl:value-of>
                                        </ultimateSourceTable>
                                       <ultimateSourceSchema></ultimateSourceSchema> <ultimateSourceColumn> 
                                            <xsl:value-of
                                                select="formula"></xsl:value-of>
                                        </ultimateSourceColumn>
                                        <ultimateSourceSystem></ultimateSourceSystem>
                                        <ultimateSourceApplication></ultimateSourceApplication>
                                    </xsl:element>
                                </xsl:for-each>
                            </ultimateSourceList>
                            <!-- /Table -->
                            <technicalInfo>
                               <techSystem>CEM</techSystem>
                            <techGroup></techGroup>
                            </technicalInfo>
                        </table>
                    </tableList>
                </schema>
            </schemaList>
        </dataBase>
        <execuTableList>
             <xsl:for-each
              select="key('kElsByGroup', tablename)[generate-id() = generate-id(key('kElsByGroupExecList', concat(tablename, '|', jobidlist))[1])]">
            <xsl:if test="jobidlist!=''">
            <execuTable>
                <execuTableInfo>
                    <execuTableName>
                        <xsl:value-of select="jobidlist"></xsl:value-of>
                    </execuTableName>
                    <description> </description>
                    <type>
                        <xsl:value-of select="level"></xsl:value-of>
                    </type>
                </execuTableInfo>
                <outputDatasets>
                    <dataset>
                        <xsl:value-of select="tablename"></xsl:value-of>
                    </dataset>
                </outputDatasets>
                <inputDatasets>
                <xsl:for-each
                   select="key('kElsByGroup', tablename)[generate-id() = generate-id(key('kElsByGroupWrittenByList', concat(tablename, '|', writtenbylist))[1])]">
                    <dataset>
                        <xsl:value-of select="writtenbylist"></xsl:value-of>
                    </dataset>
                </xsl:for-each>
                </inputDatasets>
                <transformation>
                    <columnList>
                     <xsl:for-each
              select="key('kElsByGroup', tablename)[generate-id() = generate-id(key('kElsByGroupTransformList', concat(tablename, '|', columnname,'|',writtenbylist, '|', formula))[1])]">  
                              <xsl:if test="writtenbylist!=''">
                               <xsl:if test="formula!=''">  
                                <xsl:if test="formula!=' '">  
                                <xsl:element name="column">
                                <outputDataset>
                                    <xsl:value-of select="tablename"></xsl:value-of>
                                </outputDataset>
                                <outputColumn>
                                    <xsl:value-of select="columnname"></xsl:value-of>
                                </outputColumn>
                                <inputDataset>
                                    <xsl:value-of select="writtenbylist"></xsl:value-of>
                                </inputDataset>
                                <inputColumn>
                                    <xsl:value-of select="formula"></xsl:value-of>
                                </inputColumn>
                                </xsl:element>
                                </xsl:if>
                                </xsl:if>
                               </xsl:if>
                    </xsl:for-each>        
                    </columnList>
                </transformation>
            </execuTable>
            </xsl:if>
            </xsl:for-each>
        </execuTableList>

    </xsl:template>
    <xsl:template
        match="row[not(generate-id()=generate-id(key('kElsByGroup',tablename)[1]))]" />

</xsl:stylesheet>
