.PHONY: install

install:
	mkdir -p /etc/yig/billing
	cp billing.toml /etc/yig/billing/
	cp fetch_usage.sh /etc/yig/billing/
	cp yig-billing.service /usr/lib/systemd/system/
	export GO111MODULE=on
	go build
	cp yig-billing /usr/bin
	systemctl enable yig-billing

.PHONY: run

run:
	systemctl start yig-billing

.PHONY: stop

stop:
	systemctl stop yig-billing


