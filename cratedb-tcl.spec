#
# spec file for package cratedb-tcl
#

Name:           cratedb-tcl
BuildRequires:  tcl
Version:        0.4
Release:        0
Summary:        Tcl extension and TDBC driver for CrateDB database
Url:            https://github.com/ray2501/cratedb-tcl
License:        MIT
Group:          Development/Libraries/Tcl
BuildArch:      noarch
BuildRequires:  tcl >= 8.6
BuildRequires:  rl_json
Requires:       tcl >= 8.6
Requires:       tcllib
Requires:       rl_json
BuildRoot:      %{_tmppath}/%{name}-%{version}-build
Source0:        %{name}.tar.gz

%description
Tcl extension and TDBC driver for CrateDB database.

%prep
%setup -q -n %{name}

%build

%install
dir=%buildroot%tcl_noarchdir
tclsh ./installer.tcl -path $dir
mv %buildroot%tcl_noarchdir/cratedb %buildroot%tcl_noarchdir/cratedb%version

%files
%defattr(-,root,root)
%tcl_noarchdir/cratedb%version

%changelog

