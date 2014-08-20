Landmark Translator
========

Teiid translator for Landmark JDBC driver. Landmark provides well data for oil/gas insdustries see https://www.landmarksoftware.com/Pages/DefaultHome.aspx


Installation Directions

- Clone or copy the zip file of this repository

```
cd landmark
cd translator-landmark
mvn clean install
unzip target/translator-landmark-1.0-SNAPSHOT-jboss-as7-dist.zip /path/to/${jboss-as}
```
 - Now edit the standalone-teiid.xml file, in the "teiid" subsystem, and add

<translator name="landmark" module="org.jboss.teiid.translator.landmark"/>

- Besure deploy the LandMark JDBC driver, and create a data source in standalone-teiid.xml. It is exactly same as any other JDBC data source. 

- For example assume the JNDI name as "java://landmarkDS"

Now use the "landmark" as translator name and "java://landmarkDS" as connection-jndi-name.

ex:
```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<vdb name="LandmarkVDB" version="1">
  <model name="landmark">
    <source name="conector" translator-name="landmark" connection-jndi-name="java:/landmarkDS"/>
  </model>
</vdb>
```
