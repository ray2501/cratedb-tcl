#
# Tcl extension for CrateDB
#
#     Crate provides a HTTP Endpoint that can be used to submit SQL queries.
#

package require Tcl 8.6
package require TclOO
package require TclCurl
package require rl_json

package provide CrateDB 0.1

oo::class create CrateDB {
    variable html_result
    variable host
    variable port
    variable url
    variable schema
    variable sql
    variable params
    variable cols
    variable col_types
    variable totalrows
    variable rowcount
    variable row_counter
    
    constructor {HOST PORT {SCHEMA "doc"}} {
        set html_result ""
        set host $HOST
        set port $PORT
        set schema $SCHEMA
        set url "http://$HOST:$PORT/_sql?types"
        set sql ""
        set params [dict create]
        set cols {}
        set col_types {}
        set totalrows {}
        set rowcount 0
        set row_counter 0
    }
    
    destructor {
    } 

    method httpPost {} {
        variable data
        variable curlHandle
        variable headers
        variable count
        variable param_json_array

        set data [rl_json::json new object "stmt" "string \"$sql\""]
        set count [dict size $params]
        if {$count > 0} {
             set param_json_array [list]
             for {set index 0} {$index < $count} {incr index} {
                # Check if not exists the index, maybe user skips it.
                if {[catch {set p_index [dict get $params $index]}]} {
                    lappend param_json_array "null"
                    continue
                }
                set p_type [lindex $p_index 0]

                switch -exact -- $p_type {
                  "boolean" {
                     lappend param_json_array "boolean [lindex $p_index 1]"
                  }
                  "string" {
                     lappend param_json_array "string \"[lindex $p_index 1]\""
                  }
                  "ip" {
                     lappend param_json_array "string \"[lindex $p_index 1]\""
                  }
                  "timestamp" {
                     lappend param_json_array "string \"[lindex $p_index 1]\""
                  }
                  "byte" {
                     lappend param_json_array "number [lindex $p_index 1]"
                  }
                  "short" {
                     lappend param_json_array "number [lindex $p_index 1]"
                  }
                  "integer" {
                     lappend param_json_array "number [lindex $p_index 1]"
                  }
                  "float" {
                     lappend param_json_array "number [lindex $p_index 1]"
                  }
                  "double" {
                     lappend param_json_array "number [lindex $p_index 1]"
                  }
                  "null" {
                     lappend param_json_array "null"
                  } default {
                     lappend param_json_array "string [lindex $p_index 1]"
                  }
                }
             }

             rl_json::json set data "args" [rl_json::json new "array" {*}$param_json_array]
        }

        set curlHandle [curl::init]
        set headers [list "Content-Type: application/json" "Default-Schema: $schema"]
        $curlHandle configure -url $url -bodyvar html_result -post 1 \
                          -postfields $data -httpheader $headers 
        catch { $curlHandle perform } curlErrorNumber
        if { $curlErrorNumber != 0 } {
           return -code error [curl::easystrerror $curlErrorNumber]
        }
        $curlHandle cleanup

        return -code ok
    }

    #
    # Get the raw results
    #
    method getRawResults {} {
        return $html_result
    }

    #
    # Just save the SQL code (not really send to server)
    #
    method prepare {SQL} {
        set sql $SQL
    }

    #
    # For Parameter Substitution, setup the parameter
    #
    method param {INDEX TYPE VALUE} {
        dict set params $INDEX [list $TYPE $VALUE]
    }

    #
    # Send our query to CrateDB HTTP Endpoint
    #
    method execute {} {
       if {[catch {my httpPost} errMsg]==0} {
           if {[::rl_json::json exists $html_result error]==1} {
               if {[catch {set errMsg [::rl_json::json get $html_result error message]}]==0} {
                   return -code error $errMsg
               } else {
                   return -code error "Response returns ERROR"
               }
           }

           if {[catch {set rowcount [rl_json::json get $html_result rowcount]}]} {
               set rowcount 0
           }
           set row_counter 0
           if {[catch {set totalrows [rl_json::json get $html_result rows]}]} {
               set totalrows {}
           }

           if {[catch {set cols [rl_json::json get $html_result cols]}]} { 
               set cols {}
           }

           if {[catch {set col_types [rl_json::json get $html_result col_types]}]} { 
               set col_types {}
           }

           unset params
           set params [dict create]
           return -code ok
       } else {
           unset params
           set params [dict create]
           return -code error $errMsg
       }
    }

    method getRowCount {} {
       return $rowcount
    }

    #
    # Return a row result
    #
    method getResultByList {} {
       variable old_counter

       if {[llength $cols]==0} {
         return -code error "No Result"
       }

       if {$row_counter < $rowcount} {
         set old_counter $row_counter
         incr row_counter
         return [lindex $totalrows $old_counter]
       }
    }

    #
    # Return a row result
    #
    method getResultByDict {} {
       if {[llength $cols]==0} {
         return -code error "No Result"
       }

       if {$row_counter < $rowcount} {
         set dictresult [dict create]
         set values [lindex $totalrows $row_counter]

         set length [llength $cols]
         for {set i 0} {$i < $length} {incr i} {
              dict set dictresult [lindex $cols $i] [lindex $values $i]
         }
       }

       incr row_counter
       return $dictresult
    }

    #
    # Get column names
    #
    method getColumns {} {
       return $cols
    }

    method getColumnType {INDEX} {
       variable length

       set length [llength $col_types]
       if {$length <= $INDEX} {
          return -code error "out of range"
       }

       set type [lindex $col_types $INDEX]
       if {$type == 0} {
          return "null"
       } elseif {$type == 1} {
          return "notsupported"
       } elseif {$type == 2} {
          return "byte"
       } elseif {$type == 3} {
          return "boolean"
       } elseif {$type == 4} {
          return "string"
       } elseif {$type == 5} {
          return "ip"
       } elseif {$type == 6} {
          return "double"
       } elseif {$type == 7} {
          return "float"
       } elseif {$type == 8} {
          return "short"
       } elseif {$type == 9} {
          return "integer"
       } elseif {$type == 10} {
          return "long"
       } elseif {$type == 11} {
          return "timestamp"
       } elseif {$type == 12} {
          return "object"
       } elseif {$type == 13} {
          return "geopoint"
       } elseif {$type == 100} {
          return "array"
       } elseif {$type == 101} {
          return "set"
       }
    }
}

