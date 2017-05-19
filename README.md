cratedb-tcl
=====

Tcl extension and TDBC driver for CrateDB database 

[CrateDB](https://crate.io/) is a distributed SQL database management system
that integrates a fully searchable document oriented data store.
CrateDB is designed for high scalability and includes components from
Facebook Presto, Apache Lucene, Elasticsearch and Netty.

CrateDB's language is SQL but it uses the document-oriented approach of NoSQL
style databases. CrateDB uses the SQL parser from Facebook Presto, its own query
analysis and distributed query engine. Elasticsearch and Lucene is used for the
transport protocol and cluster discovery and Netty for asynchronous event driven
network application framework.

This extension needs Tcl 8.6, TclOO, TclCurl and
[rl_json](https://github.com/RubyLane/rl_json), and tcllib sha1 package.


License
=====

This extension is Licensed under MIT license.


Implement commands
=====

## CrateDB

`CrateDB` is a TclOO class, accepts `HOST`, `PORT` and
`SCHEMA` (optional, the default schema is doc)
parameter to create an instance, and has below methods:

httpPost (internal function)  
getRawResults (internal function)  
prepare SQL  
param INDEX TYPE VALUE  
execute  
getRowCount  
getResultByList  
getResultByDict  
getColumns  
getColumnType INDEX  
getDigest STRING  
deleteBlob TABLE DIGEST  
putBlob TABLE DIGEST DATA  
getBlob TABLE DIGEST  
isBlobExist TABLE DIGEST  

`prepare` just gets the SQL code and store it to a variable.
So this mehtod does not send request to server.

`param` is used to setup SQL parameter substitution.
TYPE supports boolean, string, ip, timestamp, byte, short, integer,
float, double (and null).

`execute` is using TclCurl to post our request to CrateDB HTTP Endpoint.

`getReulstByList` and `getResultByDict` is using to get result set.
Please notice, I do not know how to handle NULL correctly.
So result dict still gets the key but value is empty.


## TDBC commands

tdbc::cratedb::connection create db host port ?schema? ?-option value...?

Create a CrateDB database connection handle is established by invoking
`tdbc::cratedb::connection create`, passing it the name to be used as a
connection handle, followed by a host name, port number and
schema name (optional).

The tdbc::cratedb::connection create object command supports the -encoding, -isolation and
-readonly option (only gets the default setting).

CratDB driver for TDBC implements a statement object that represents a SQL statement in a
database. Instances of this object are created by executing the `prepare` or
`preparecall` object command on a database connection.

The prepare object command against the connection accepts arbitrary SQL code to be executed
against the database.

The paramtype object command allows the script to specify the type and direction of parameter
transmission of a variable in a statement. Now CrateDB driver only specify the type work.

CrateDB driver paramtype accepts below type:
boolean, string, ip, timestamp, byte, short, integer, float, double

The `execute` object command executes the statement.


Examples
=====

## CrateDB

Basic example:

    package require CrateDB

    set crate [CrateDB new localhost 4200]

    $crate prepare "select name from sys.cluster"
    if {[catch {$crate execute} errMsg]==0} {
        set count [$crate getRowCount]
        for {set i 0} {$i < $count} {incr i} {
            puts "name: [dict get [$crate getResultByDict] name]"
        }
    } else {
        puts "Error: $errMsg"
    }

    $crate destroy

BLOB function example:

    package require CrateDB

	set crate [CrateDB new localhost 4200]

	# Creating a table for blobs with a custom blob data path
	$crate prepare "create blob table myblobs clustered into 3 shards with \
		        (blobs_path='/tmp/crate_blob_data')"
	if {[catch {$crate execute} errMsg]} {
	    puts "Error: $errMsg"
	}

	# Prepare our content and digest
	set digest [$crate getDigest "Hello CrateDB"]

	# The blob can now be uploaded by issuing a PUT request
	if {[catch {$crate putBlob myblobs $digest "Hello CrateDB"} errMsg]} {
	    puts "Put Blob ERROR: $errMsg"
	}

	# Pug again, test if a blob already exists
	if {[catch {$crate putBlob myblobs $digest "Hello CrateDB"} errMsg]} {
	    puts "Put Blob ERROR: $errMsg"
	}

	# To determine if a blob exists without downloading it
	if {[catch {$crate isBlobExist myblobs $digest} errMsg]==0} {
	    puts "Blob exists!!!"
	} else {
	    puts "Blob ERROR: $errMsg"
	}

	# To download a blob simply use a GET request
	if {[catch {set content [$crate getBlob myblobs $digest]} errMsg]==0} {
	    puts "Get Blob: $content"
	} else {
	    puts "Get Blob ERROR: $errMsg"
	}

	# To list all blobs inside a blob table a SELECT statement can be used
	puts "Query the table to list our blobs:"
	$crate prepare "select digest, last_modified from blob.myblobs"
	if {[catch {$crate execute} errMsg]==0} {
	    set count [$crate getRowCount]
	    for {set i 0} {$i < $count} {incr i} {
		puts "digest : [dict get [$crate getResultByDict] digest ]"
	    }    
	}

	# To delete a blob simply use a DELETE request
	if {[catch {$crate deleteBlob myblobs $digest} errMsg]} {
	    puts "Put Blob ERROR: $errMsg"
	}

	# Blob tables can be deleted similar to normal tables
	$crate prepare "drop blob table myblobs"
	if {[catch {$crate execute} errMsg]} {
	    puts "Error: $errMsg"
	}

	$crate destroy

## TDBC

    package require tdbc::cratedb

    tdbc::cratedb::connection create db localhost 4200

    set statement [db prepare \
        {create table if not exists person (id integer primary key, name string not null)}]
    $statement execute
    $statement close

    set statement [db prepare {insert into person (id, name) values(1, 'leo')}]
    $statement execute
    $statement close

    set statement [db prepare {insert into person (id, name) values(2, 'yui')}]
    $statement execute
    $statement close

    set statement [db prepare {insert into person (id, name) values(:id, :name)}]
    $statement paramtype id integer
    $statement paramtype name string

    set id 3
    set name danilo
    $statement execute

    set myparams [dict create id 4 name arthur]
    $statement execute $myparams
    $statement close

    # It is necessary to refresh
    set statement [db prepare {REFRESH table person}]
    $statement execute
    $statement close

    set statement [db prepare {SELECT * FROM person order by id}]

    $statement foreach row {
        puts [dict get $row id]
        puts [dict get $row name]
    }

    $statement close

    set statement [db prepare {drop table if exists person}]
    $statement execute
    $statement close

    db close

