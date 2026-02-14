#
# Tcl package index file
#
package ifneeded CrateDB 0.5 \
[list source [file join $dir cratedb.tcl]]

package ifneeded tdbc::cratedb 0.5 \
[list source [file join $dir tdbccratedb.tcl]]

