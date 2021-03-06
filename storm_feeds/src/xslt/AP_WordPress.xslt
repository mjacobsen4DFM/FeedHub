<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:nitf="http://iptc.org/stdnitf/2006-10-18/"
                xmlns:atom="http://www.w3.org/2005/Atom" 
                xmlns:apcm="http://ap.org/schemas/03/2005/apcm" 
                xmlns:apnm="http://ap.org/schemas/03/2005/apnm" 
                xmlns:georss="http://www.georss.org/georss" 
                xmlns:o="http://w3.org/ns/odrl/2/"
                exclude-result-prefixes="nitf atom apcm apnm georss o">
  <xsl:output method="xml" indent="yes"/>

  <xsl:template match="@* | node()">
    <xsl:variable name="apos">'</xsl:variable>
    <xsl:variable name="quot">"</xsl:variable>
    <wp-api>
      <guid>
        <xsl:value-of select="normalize-space(/atom:entry/apnm:NewsManagement/apnm:ManagementId)"/>
      </guid>
      <sequence type="int">
        <xsl:value-of select="normalize-space(/atom:entry/apnm:NewsManagement/apnm:ManagementSequenceNumber)"/>
      </sequence>
      <date>
        <xsl:variable name="date" select="/atom:entry/atom:content/nitf/head/docdata/date.issue/@norm"/>
        <xsl:value-of select="normalize-space(concat(substring($date, 1,4), '-',substring($date, 5,2), '-',substring($date, 7,2), 'T', substring($date, 10,2), ':',substring($date, 12,2), ':',substring($date, 14,2), '+00:00'))"/>
      </date>
      <title>
        <xsl:value-of select="normalize-space(/atom:entry/atom:content/nitf/body/body.head/hedline/hl1)"/>
      </title>
      <byline>
        <xsl:value-of select="normalize-space(/atom:entry/atom:content/nitf/body/body.head/byline/text())"/>
      </byline>
      <source>
        <xsl:value-of select="normalize-space(/atom:entry/atom:content/nitf/body/body.head/distributor/text())"/>
      </source>
      <content>
        <xsl:if test="/atom:entry/atom:content/nitf/body/body.content/block[@id='Main']/p">
          <xsl:for-each select="/atom:entry/atom:content/nitf/body/body.content/block[@id='Main']/p">
            <xsl:text>&lt;p&gt;</xsl:text>
            <xsl:value-of select="normalize-space(.)"/>
            <xsl:text>&lt;/p&gt;</xsl:text>
          </xsl:for-each>
        </xsl:if>
        <xsl:if test="/atom:entry/atom:content/nitf/body/body.content/block[@id='Main']/hl2">
          <xsl:for-each select="/atom:entry/atom:content/nitf/body/body.content/block[@id='Main']">
            <xsl:text>&lt;p&gt;</xsl:text>
            <xsl:value-of select="normalize-space(hl2)"/>
            <xsl:text>&lt;/p&gt;</xsl:text>
          </xsl:for-each>
        </xsl:if>
      </content>
      <excerpt>
        <xsl:if test="/atom:entry/atom:content/nitf/body/body.content/block[@id='Main']/p">
          <xsl:value-of select="normalize-space(/atom:entry/atom:content/nitf/body/body.content/block[@id='Main']/p[1])"/>
        </xsl:if>
        <xsl:if test="/atom:entry/atom:content/nitf/body/body.content/block[@id='Main']/hl2">
          <xsl:text>HEADLINES ONLY</xsl:text>
        </xsl:if>
      </excerpt>
      <images>
        <xsl:for-each select="/atom:entry/atom:content/nitf/body/body.content/media[@media-type='Photo']">
          <image>
            <xsl:for-each select="media-reference[@name='AP Photo']">
                <mime-type>
                  <xsl:value-of select="normalize-space(@mime-type)"/>
                </mime-type>
                <source>
                  <xsl:value-of select="normalize-space(@source)"/>
                </source>
            </xsl:for-each>
            <guid>
              <xsl:value-of select="normalize-space(media-metadata[@name='managementId']/@value)"/>
            </guid>
            <caption>
              <xsl:for-each select="media-caption/p">                
                <xsl:value-of select="normalize-space(concat('&lt;p&gt;', normalize-space(.), '&lt;/p&gt;'))"/>
              </xsl:for-each>
            </caption>
            <name>
              <xsl:value-of select="normalize-space(media-metadata[@name='OriginalFileName']/@value)"/>
            </name>
            <title>
              <xsl:value-of select="normalize-space(substring-before(media-metadata[@name='OriginalFileName']/@value, '.'))"/>
            </title>
            <credit>
              <xsl:value-of select="normalize-space(media-producer)"/>              
            </credit>
          </image>
        </xsl:for-each>
      </images>
    </wp-api>
  </xsl:template>
</xsl:stylesheet>
