<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" version="1.0" encoding="UTF-8"
        indent="yes" />
		
    <xsl:key name="table" match="row" use="tablename"/>

	<xsl:key name="col" match="row" use="concat(tablename, '|', columnname)"/>
	
	<xsl:template match="table">
        <stc>
            <xsl:apply-templates select="row[generate-id() = generate-id(key('table', tablename)[1])]" mode="table"/>
        </stc>
    </xsl:template>
    
    <!--<xsl:template match="row">
        <xsl:apply-templates />
    </xsl:template> --> 
	
    <xsl:template match="row" mode="table">
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
                                <interval>
                                    <xsl:value-of select="interval"></xsl:value-of>
                                </interval>
                            </physicalTableName>
                            <!-- Table name="{tablename}" -->
                            <columnList>
                      <!--  RIjin  <xsl:for-each
                                    select="key('kElsByGroup',tablename)[1]">
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
                                </xsl:for-each> Rij-->
								
							<xsl:apply-templates select="key('table', tablename)[generate-id() = generate-id(key('col', concat(tablename, '|', columnname))[1])]/columnname"/>
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
                                    select="key('kElsByGroup',tablename)">
                                    <xsl:if test="position()=1">
                                        <xsl:element name="execuTable">
                                            <application>
                                                <xsl:value-of
                                                    select="readbylist"></xsl:value-of>
                                            </application>
                                            <description>
                                                <xsl:choose>
                                                    <xsl:when test="readbylist = ''">
                                                        <xsl:value-of select="readbylist" />
                                                    </xsl:when>
                                                    <xsl:otherwise>
                                                        <xsl:value-of select="readbylist"></xsl:value-of> table is loaded by reading data from 
                                                        
                                                        <xsl:value-of select="tablename"></xsl:value-of>
                                                    </xsl:otherwise>
                                                </xsl:choose>
                                            </description>
                                        </xsl:element>
                                    </xsl:if>
                                </xsl:for-each>
                            </readByList>
                            <!-- writtenByList -->
                            <writtenByList>
                                <xsl:for-each
                                    select="key('kElsByGroup',tablename)">
                                    <xsl:if test="position()=1">
                                        <xsl:element name="execuTable">
                                            <system>
                                                <xsl:value-of
                                                    select="writtenbylist"></xsl:value-of>
                                            </system>
                                            <description>
                                                <xsl:choose>
                                                    <xsl:when test="writtenbylist = ''">
                                                        <xsl:value-of select="writtenbylist " />
                                                    </xsl:when>
                                                    <xsl:otherwise>
                                                        <xsl:value-of select="writtenbylist"></xsl:value-of> table is used to load data to 
                                                        
                                                        <xsl:value-of select="tablename"></xsl:value-of>
                                                    </xsl:otherwise>
                                                </xsl:choose>
                                            </description>
                                        </xsl:element>
                                    </xsl:if>
                                </xsl:for-each>
                            </writtenByList>
                            <!-- ultimatesourcelist -->
                            <ultimateSourceList>
                                <xsl:for-each
                                    select="key('kElsByGroup',tablename)">
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
                                        <ultimateSourceColumn>
                                            <xsl:value-of
                                                select="formula"></xsl:value-of>
                                        </ultimateSourceColumn>
                                    </xsl:element>
                                </xsl:for-each>
                            </ultimateSourceList>
                            <!-- /Table -->
                        </table>
                    </tableList>
                </schema>
            </schemaList>
        </dataBase>
        <execuTableList>
            <execuTable>
                <execuTableInfo>
                    <execuTableName>
                        <xsl:value-of select="jobidlist"></xsl:value-of>
                    </execuTableName>
                </execuTableInfo>
                <outputDatasets>
                    <dataset>
                        <xsl:value-of select="tablename"></xsl:value-of>
                    </dataset>
                </outputDatasets>
                <inputDatasets>
                    <xsl:value-of select="writtenbylist"></xsl:value-of>
                </inputDatasets>
                <transformation>
                    <columnList>
                        <xsl:for-each select="key('kElsByGroup',tablename)">
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
                        </xsl:for-each>
                    </columnList>
                </transformation>
            </execuTable>
        </execuTableList>
    </xsl:template>
	<xsl:template match="row">
      <column>
          <xsl:apply-templates select="*[not(self::tablename)]" mode="source-list"/>
      </column>
  </xsl:template>

  <xsl:template match="row/columnname" mode="source-list">
      <columnName>
          <xsl:value-of select="."/>
      </columnName>
  </xsl:template>

  <xsl:template match="row/ultimateSourceTable" mode="source-list">
      <xsl:copy-of select="."/>
  </xsl:template>
    <xsl:template
        match="row[not(generate-id()=generate-id(key('kElsByGroup',tablename)[1]))]" />
</xsl:stylesheet>

