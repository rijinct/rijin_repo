<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	version="1.0">

  <xsl:output method="xml" indent="yes"/>
  
  <xsl:key name="table" match="row" use="tablename"/>

  <xsl:key name="col" match="row" use="concat(tablename, '|', columnname)"/>

  <xsl:template match="table">
      <Prod>
          <dataBase>
             <xsl:apply-templates select="row[generate-id() = generate-id(key('table', tablename)[1])]" mode="table"/>
          </dataBase>
      </Prod>
  </xsl:template>
  
  <xsl:template match="row" mode="table">
      <physicalTableName>
          <xsl:value-of select="tablename"/>
      </physicalTableName>
      <columnList>
          <xsl:apply-templates select="key('table', tablename)[generate-id() = generate-id(key('col', concat(tablename, '|', columnname))[1])]/columnname"/>
      </columnList>
      <finalSourceList>
          <xsl:apply-templates select="key('table', tablename)"/>
      </finalSourceList>
  </xsl:template>
  
  <xsl:template match="row/columnname">
      <name>
          <xsl:value-of select="."/>
      </name>
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

</xsl:stylesheet>
