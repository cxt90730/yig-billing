%global debug_package %{nil}
%global __strip /bin/true

Name:           yig-billing
Version:        %{version}
Release:        %{release}

Summary:	yig-billing is a go module of billing for yig

Source:		%{name}_%{version}-%{release}_linux_amd64.tar.gz
Group:		YIG
License:        Apache-2.0
URL:		http://github.com/cxt90730/yig-billing
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}_%{version}-%{release}-XXXXXX)

%description
Requires:       librdkafka1

%prep
%setup -q -n %{name}_%{version}-%{release}_linux_amd64


%build


%install
rm -rf %{buildroot}
install -D -m 755 yig-billing %{buildroot}%{_bindir}/yig-billing
install -D -m 644 yig-billing.service   %{buildroot}/usr/lib/systemd/system/yig-billing.service
install -D -m 644 yig-billing.logrotate %{buildroot}/etc/logrotate.d/yig-billing.logrotate
install -D -m 644 yig-billing.toml %{buildroot}%{_sysconfdir}/yig/yig-billing.toml
install -D -m 644 fetch_usage.sh %{buildroot}%{_sysconfdir}/yig/fetch_usage.sh
install -D -m 644 fetch_usage_bucket.sh %{buildroot}%{_sysconfdir}/yig/fetch_usage_bucket.sh
install -D -m 644 confluent.repo %{buildroot}/etc/yum.repos.d/confluent.repo

%post
systemctl enable yig-billing

%preun

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
/usr/bin/yig-billing
/usr/lib/systemd/system/yig-billing.service
/etc/logrotate.d/yig-billing.logrotate
%config(noreplace) /etc/yig/yig-billing.toml
%config(noreplace) /etc/yig/fetch_usage.sh
%config(noreplace) /etc/yig/fetch_usage_bucket.sh
%config(noreplace) /etc/yum.repos.d/confluent.repo

%changelog