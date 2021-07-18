#
# Tcl package index file
#
package ifneeded CrateDB 0.4 \
[list source [file join $dir cratedb.tcl]]

package ifneeded tdbc::cratedb 0.4 \
[list source [file join $dir tdbccratedb.tcl]]

