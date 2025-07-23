<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<#list itemList as item>
    <property>
        <name>${item.name}</name>
<#if item.value?contains("\\n")>
        <value>
<#list item.value?split("\\n") as line>
            ${line}
</#list>
        </value>
<#else>
        <value>${item.value}</value>
</#if>
    </property>
</#list>
</configuration>