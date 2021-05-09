<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" version="1.0" encoding="UTF-8"
		indent="yes" omit-xml-declaration="yes"/>
	<xsl:strip-space elements="*" />

	<xsl:template match="/">
    <Universe>
		<xsl:attribute name="name">
	    <xsl:value-of select="//universe/name" />
    	</xsl:attribute>
	<Attributes>
	<xsl:apply-templates />
	</Attributes>
	</Universe>

	</xsl:template>
	<xsl:template match="//dimension/item | //measure/item | //dimension[not(item)]">
		<Attribute>
			<Kpi_Name>
				<xsl:value-of select="name" />
			</Kpi_Name>
			<Definition>
				<xsl:value-of select="description" />
			</Definition>
			<Descriptive_Example>
				<xsl:value-of select="NA" />
			</Descriptive_Example>
			<Business_Rule>
				<xsl:value-of select="NA" />
			</Business_Rule>
			<Calculation_Rule>
				<xsl:value-of select="SQL_Definition" />
			</Calculation_Rule>
			<LogicalTable>
				<xsl:value-of select="attribute" />
			</LogicalTable>
			<LogicalColumn>
				<xsl:value-of select="SQL_Definition" />
			</LogicalColumn>
			<Asset_Type>KPI</Asset_Type>
			<DOMAIN_NAME>CEM KPIs</DOMAIN_NAME>
			<DOMAIN_TYPE>Business Asset Domain</DOMAIN_TYPE>
			<COMMUNITY>
				<xsl:value-of select="NA" />
			</COMMUNITY>
		</Attribute>
	</xsl:template>

	<xsl:template match="universe/folder/folder/item/folder/name" />
	<xsl:template match="universe/tables" />
	<xsl:template match="universe/headers" />
	<xsl:template match="description" />
	<xsl:template match="name" />
	<xsl:template match="exposed" />
</xsl:stylesheet>
