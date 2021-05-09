<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output omit-xml-declaration="yes" indent="yes" />
	<xsl:strip-space elements="*" />

	<xsl:template match="/">
		<execuTableList>
			<xsl:apply-templates />
		</execuTableList>
	</xsl:template>
	<xsl:template match="row">
		<execuTable>
			<execuTableInfo>
				<execuTableName>
					<xsl:value-of select="job_name" />
				</execuTableName>
			</execuTableInfo>
			<execuTableResult>
				<executionTime>
					<xsl:value-of select="start_time" />
				</executionTime>
				<jobstatus>
					<xsl:value-of select="status" />
				</jobstatus>
				<recordsProcessed>
					<xsl:value-of select="recordsProcessed" />
				</recordsProcessed>
				<recordsRejected>
					<xsl:value-of select="recordsRejected" />
				</recordsRejected>
				<recordsDuplicated>
					<xsl:value-of select="recordsDuplicated" />
				</recordsDuplicated>
			</execuTableResult>
		</execuTable>
	</xsl:template>
</xsl:stylesheet>