#!/usr/bin/tclsh
#
# It is a very simple installer for cratedb-tcl package
#

if {[catch {package require Tcl 8.6} errMsg]} {
  puts $errMsg
  exit
}

if {[catch {package require tdbc} errMsg]} {
  puts $errMsg
  exit
}

if {[catch {package require rl_json} errMsg]} {
  puts $errMsg
  exit
}
 
# Use info library to get the library path, setup default value
set path [file normalize [info library]/../]
set srcFolder "cratedb"
set uninstall 0

if {$argc > 0 && [llength $argv] % 2 eq 0} {
  foreach {key value} $argv {
    if {[string compare [string tolower $key] "-path"]==0} {
      set path $value
    } elseif {[string compare [string tolower $key] "-uninstall"]==0} {
      set uninstall $value
    }
  }
} elseif {[llength $argv] % 2 eq 1} {
  puts "Parameters are not correct, close..."
  exit
}

puts "Install path is - $path"
puts "Now check path exists or not..."
if {[file exists $path]} {
  puts "Done, path exists."
  if {$uninstall eq 1} {
    if {[file exists "$path/$srcFolder"]} {
      puts "Now remove folder $srcFolder..."
      if {[catch {file delete -force $path/$srcFolder} errMsg]} {
        puts $errMsg
      } else {
        puts "Done."
      }
    } else {
      puts "$path/$srcFolder does not exist, it is not necessary to remove it."
    }
  } else {
    if {[file exists $srcFolder]} {
      puts "Now copy folder $srcFolder..."
      if {[catch {file copy -force $srcFolder $path} errMsg]} {
        puts $errMsg
      } else {
        puts "Done."
      }
    } else {    
      puts "Sorry, $srcFolder does not exist!!!"
    }
  }
} else {
  puts "Path does not exist!!!"
  puts "Now try to use mkdir..."
  if {[catch {file mkdir $path} errMsg]} {
    puts $errMsg
  } else {
    puts "Done."
    if {[file exists $srcFolder]} {
      puts "Now copy folder $srcFolder..."
      if {[catch {file copy -force $srcFolder $path} errMsg]} {
        puts $errMsg
      } else {
        puts "Done."
      }
    } else {
      puts "Sorry, $srcFolder does not exist!!!"
    }
  }
}
