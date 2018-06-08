#------------------------------------------------------------------------------
#
#	Tcl DataBase Connectivity CrateDB Driver
#	Class definitions and Tcl-level methods for the tdbc::cratedb bridge.
#
#------------------------------------------------------------------------------

package require Tcl 8.6
package require tdbc
package require CrateDB

package provide tdbc::cratedb 0.3


::namespace eval ::tdbc::cratedb {

    namespace export connection

}

#------------------------------------------------------------------------------
#
# tdbc::cratedb::connection --
#
#	Class representing a connection to a CrateDB database.
#
#-------------------------------------------------------------------------------


::oo::class create ::tdbc::cratedb::connection {

    superclass ::tdbc::connection

    variable cratedb

    constructor {host port {schema "doc"} args} {
        next

        if {[llength $args] % 2 != 0} {
            set cmd [lrange [info level 0] 0 end-[llength $args]]
            return -code error \
            -errorcode {TDBC GENERAL_ERROR HY000 CRATEDB WRONGNUMARGS} \
            "wrong # args, should be \"$cmd ?-option value?...\""
        }

        set cratedb [CrateDB new $host $port $schema]


        if {[llength $args] > 0} {
	    my configure {*}$args
        }
    }

    forward statementCreate ::tdbc::cratedb::statement create

    method configure args {
        if {[llength $args] == 0} {
            set result {-encoding utf-8}
            lappend result -isolation readuncommitted
            lappend result -readonly 0
            return $result
        } elseif {[llength $args] == 1} {
	    set option [lindex $args 0]
	    switch -exact -- $option {
		-e - -en - -enc - -enco - -encod - -encodi - -encodin - 
		-encoding {
		    return utf-8
                }
		-i - -is - -iso - -isol - -isola - -isolat - -isolati -
		-isolatio - -isolation {
		    return readuncommitted
                }
                -r - -re - -rea - -read - -reado - -readon - -readonl -
		-readonly {
		    return 0
                }
		default {
		    return -code error \
			-errorcode [list TDBC GENERAL_ERROR HY000 CrateDB \
					BADOPTION $option] \
			"bad option \"$option\": must be\
                         -encoding, -isolation or -readonly"
                }
            }
          } elseif {[llength $args] % 2 != 0} {
	    # Syntax error
	    set cmd [lrange [info level 0] 0 end-[llength $args]]
	    return -code error \
		-errorcode [list TDBC GENERAL_ERROR HY000 \
				CrateDB WRONGNUMARGS] \
		"wrong # args, should be \" $cmd ?-option value?...\""
         }

	# Set one or more options

	foreach {option value} $args {
	    switch -exact -- $option {
		-e - -en - -enc - -enco - -encod - -encodi - -encodin - 
		-encoding {
		    if {$value ne {utf-8}} {
			return -code error \
			    -errorcode [list TDBC FEATURE_NOT_SUPPORTED 0A000 \
					    CrateDB ENCODING] \
			    "-encoding not supported to setup."
		    }
		}
		-i - -is - -iso - -isol - -isola - -isolat - -isolati -
		-isolatio - -isolation {
		    if {$value ne {readuncommitted}} {
			return -code error \
			    -errorcode [list TDBC FEATURE_NOT_SUPPORTED 0A000 \
					    CrateDB ISOLATION] \
			    "-isolation not supported to setup."
		    }
		}
		-r - -re - -rea - -read - -reado - -readon - -readonl -
		-readonly {
		    if {$value} {
			return -code error \
			    -errorcode [list TDBC FEATURE_NOT_SUPPORTED 0A000 \
					    CrateDB READONLY] \
			    "-readonly not supported to setup."
		    }
		}
		default {
		    return -code error \
			-errorcode [list TDBC GENERAL_ERROR HY000 \
					CrateDB BADOPTION $value] \
			"bad option \"$option\": must be\
                         -encoding, -isolation or -readonly"
		}
	    }
	}
        return
    }

    # invoke close method -> destroy our object
    method close {} {
        set mystats [my statements]
        foreach mystat $mystats {
            $mystat close
        }
        unset mystats

        $cratedb destroy

        next
    }


    #
    # Info from https://crate.io/docs/reference/sql/information_schema.html
    #
    method tables {{pattern %}} {
        set retval {}

        # Got table id, table name and table schema
        $cratedb prepare "SELECT table_schema, \
                   table_name as name, number_of_shards, number_of_replicas, \
                   blobs_path from information_schema.tables \
                   where table_name like '$pattern' AND \
                   (table_schema != 'information_schema' AND \
                   table_schema != 'pg_catalog' AND table_schema != 'sys')"

        $cratedb execute
        set count [$cratedb getRowCount]
        for {set i 0} { $i < $count} {incr i 1} {
            set row [$cratedb getResultByDict]
            dict set row name [string tolower [dict get $row name]]
            dict set retval [dict get $row name] $row
        }

        return $retval
    }

    method columns {table {pattern %}} {
        set retval {}

        # Setup our pattern
        set pattern [string map [list \
                                     * {[*]} \
                                     ? {[?]} \
                                     \[ \\\[ \
                                     \] \\\[ \
                                     _ ? \
                                     % *] [string tolower $pattern]]
        $cratedb prepare "select column_name as name, ordinal_position as pos, \
                   data_type as type from information_schema.columns \
                   where table_name = '$table'"
        $cratedb execute

        set count [$cratedb getRowCount]
        for {set i 0} { $i < $count} {incr i 1} {
            set row [$cratedb getResultByDict]
            dict set row name [string tolower [dict get $row name]]

            set column_name [dict get $row name]
            if {![string match $pattern $column_name]} {
                continue
            }

            dict set retval [dict get $row name] $row
        }

        return $retval
    }

    method primarykeys {table} {
        set retval {}
        $cratedb prepare "select table_schema, table_name, constraint_name as name \
                   from information_schema.table_constraints \
                   where table_name = '$table' AND constraint_type='PRIMARY_KEY'"

        # Got table name, table schema and key name
        $cratedb execute
        set retval [dict create]
 
        set count [$cratedb getRowCount]
        for {set i 0} { $i < $count} {incr i 1} {
            set row [$cratedb getResultByDict]

            # Get table name
            dict set retval tableSchema [dict get $row table_schema]
            dict set retval tableName [dict get $row table_name]

            # Get key name
            dict set row name [string tolower [dict get $row name]]
            set key_name [dict get $row name]
            dict set retval keyName $key_name
        }

        return $retval
    }

    method foreignkeys {args} {
        return -code error "Feature is not supported"
    }

    # The 'prepareCall' method gives a portable interface to prepare
    # calls to stored procedures.  It delegates to 'prepare' to do the
    # actual work.
    method preparecall {call} {
        regexp {^[[:space:]]*(?:([A-Za-z_][A-Za-z_0-9]*)[[:space:]]*=)?(.*)} \
            $call -> varName rest
        if {$varName eq {}} {
            my prepare \\{$rest\\}
        } else {
            my prepare \\{:$varName=$rest\\}
        }
    }

    # The 'begintransaction' method launches a database transaction
    method begintransaction {} {
        return -code error "Feature is not supported"
    }


    # The 'commit' method commits a database transaction
    method commit {} {
        return -code error "Feature is not supported"
    }


    # The 'rollback' method abandons a database transaction
    method rollback {} {
        return -code error "Feature is not supported"
    }


    method prepare {sqlCode} {
        set result [next $sqlCode]
        return $result
    }


    method getDBhandle {} {
        return $cratedb
    }
}

#------------------------------------------------------------------------------
#
# tdbc::cratedb::statement --
#
#	The class 'tdbc::cratedb::statement' models one statement against a
#       database accessed through a cratedb connection
#
#------------------------------------------------------------------------------

::oo::class create ::tdbc::cratedb::statement {

    superclass ::tdbc::statement

    variable Params ConnectionI sql stmt

    constructor {connection sqlcode} {
        next
        set Params {}
        set ConnectionI [$connection getDBhandle]
        set sql {}
        foreach token [::tdbc::tokenize $sqlcode] {

            # I have no idea how to get params meta here,
            # just give a default value.
            if {[string index $token 0] in {$ : @}} {
                dict set Params [string range $token 1 end] \
                    {type string direction in}

                append sql "?"
                continue
            }

            append sql $token
        }

        $ConnectionI prepare $sql
    }

    forward resultSetCreate ::tdbc::cratedb::resultset create

    method close {} {
        set mysets [my resultsets]
        foreach myset $mysets {
            $myset close
        }
        unset mysets

        next
    }

    # The 'params' method returns descriptions of the parameters accepted
    # by the statement
    method params {} {
        return $Params
    }


    method paramtype args {
        set length [llength $args]

        if {$length < 2} {
            set cmd [lrange [info level 0] 0 end-[llength $args]]
            return -code error \
            -errorcode {TDBC GENERAL_ERROR HY000 cratedb WRONGNUMARGS} \
            "wrong # args...\""
        }

        set parameter [lindex $args 0]
        if { [catch  {set value [dict get $Params $parameter]}] } {
            set cmd [lrange [info level 0] 0 end-[llength $args]]
            return -code error \
            -errorcode {TDBC GENERAL_ERROR HY000 cratedb BADOPTION} \
            "wrong param...\""
        }

        set count 1
        if {$length > 1} {
            set direction [lindex $args $count]

            if {$direction in {in out inout}} {
                # I don't know how to setup direction, setup to in
                dict set value direction in
                incr count 1
            }
        }

        if {$length > $count} {
            set type [lindex $args $count]

            # Only accept these types
            if {$type in {string ip timestamp byte short integer long \
                          float double boolean geopoint array}} {
                dict set value type $type
            }
        }

        # Skip other parameters and setup
        dict set Params $parameter $value

    }


    method getDBhandle {} {
        return $ConnectionI
    }
}

#------------------------------------------------------------------------------
#
# tdbc::cratedb::resultset --
#
#	The class 'tdbc::cratedb::resultset' models the result set that is
#	produced by executing a statement against a cratedb database.
#
#------------------------------------------------------------------------------

::oo::class create ::tdbc::cratedb::resultset {

    superclass ::tdbc::resultset

    variable -set {*}{
        -connectionI -sqltypes -params -RowCount -count
         -columns
    }


    constructor {statement args} {
        next
    	set -connectionI [$statement getDBhandle]
        set -params  [$statement params]
        set -columns {}
        set -sqltypes {}
        set -count 0

        if {[llength $args] == 0} {

            set keylist [dict keys ${-params}]
            set count 0

            foreach mykey $keylist {
               if {[info exists ::$mykey] == 1} {
                   upvar 1 $mykey mykey1
                   set -sqltypes [dict get [dict get ${-params} $mykey] type]
                   ${-connectionI} param $count ${-sqltypes} $mykey1
               } else {
                   ${-connectionI} param $count null ""
               }

               incr count 1
            }

            if {[catch {set result [ ${-connectionI} execute ]} errMsg]} {
               set -RowCount -1
               return -code error $errMsg
            }
            set -RowCount [${-connectionI} getRowCount]
        } elseif {[llength $args] == 1} {

            # If the dict parameter is supplied, it is searched for a key
            # whose name matches the name of the bound variable
            set -paramDict [lindex $args 0]

            set keylist [dict keys ${-params}]
            set count 0

            foreach mykey $keylist {
                 if {[catch {set bound [dict get ${-paramDict} $mykey]}]==0} {
                   set -sqltypes [dict get [dict get ${-params} $mykey] type]
                   ${-connectionI} param $count ${-sqltypes} $bound
                 } else {
                   ${-connectionI} param $count null ""
                 }

                 incr count 1
            }

            if {[catch {set result [ ${-connectionI} execute ]} errMsg]} {
               set -RowCount -1
               return -code error $errMsg
            }
            set -RowCount [${-connectionI} getRowCount]
        } else {
            return -code error \
            -errorcode [list TDBC GENERAL_ERROR HY000 \
                    CRATEDB WRONGNUMARGS] \
            "wrong # args: should be\
                     [lrange [info level 0] 0 1] statement ?dictionary?"
        }
    }


    # Return a list of the columns
    method columns {} {
        set -columns [${-connectionI} getColumns]
        return ${-columns}
    }


    method nextresults {} {
        set have 0

        # Is this method enough?
        if { ${-RowCount} > ${-count} } {
            set have 1
        } else {
            set have 0
        }

        return $have
    }


    method nextlist var {
        upvar 1 $var row
        set row {}

        variable mylist

        if { [catch {set mylist [ ${-connectionI} getResultByList ]}] } {
            return 0
        }

        if {[llength $mylist] == 0} {
            # No exception but result is empty, update -count
            incr -count 1        
            return 0
        }

        set row $mylist
        incr -count 1

        return 1
    }


    method nextdict var {
        upvar 1 $var row
        set row {}

        variable mydict

        if { [catch {set mydict [ ${-connectionI} getResultByDict ]}] } {
            return 0
        }

        if {[dict size $mydict] == 0} {
            # No exception but result is empty, update -count
            incr -count 1        
            return 0
        }

        set row $mydict
        incr -count 1

        return 1
    }


    # Return the number of rows affected by a statement
    method rowcount {} {
        return ${-RowCount}
    }
}
