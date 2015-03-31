#s6-db

## Synopsis

Classes for easy access to databases.

## Code Example

Connections can be defined with a property file. For instance if you had a property file s6db.properties in the package/dir bundles on the class path whose contents are

    Database.Primary.Driver         = org.postgresql.Driver
    Database.Primary.UserName       = <username>
    Database.Primary.Password       = <possword>
    Database.Primary.Url            = jdbc:postgresql://localhost/<db>
    Database.Primary.MaxConnections = 12

then you can use

      ResourceReader reader = new ResourceReaderImpl("bundles.s6db");
      ConnectionInfo ci = ConnectionInfo.valueOf(reader, "Primary");
      Database db = new Database(ci);
        
      Table table = db.getTable(<tablename>);
      try {
          RecordSet rs = table.getRecordSet(<whereclause>);
          
          while (rs.next()) {
              // read values from RecordSet object
          }
      } catch (DatabaseException ex) {
          ex.printStackTrace();
      } finally {
          // Must *always* release the database connection
          db.release();
      }


Much more Coming Soon

## Installation

Include the following in your maven project

    <dependency>
       <groupId>com.samsix</groupId>
       <artifactId>s6-db</artifactId>
       <version>1.0.0</version>
    </dependency>

or clone and build yourself using

    mvn clean install

## API Reference

You can create a source jar

    mvn source:jar

## Tests

Sadly, no tests at present as I never moved them out of the project they were in to this Open Source Project.
