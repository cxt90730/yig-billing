NAME=yig-billing
VERSION?=v0.0.1
RELEASE?=rc01
PWD=$(shell pwd)
ARCH=linux_amd64
BIN_NAME=$(NAME)_$(VERSION)-$(RELEASE)_$(ARCH)

.PHONY: build

build:
	go build

.PHONY: run

run:
	systemctl start yig-billing

.PHONY: stop

stop:
	systemctl stop yig-billing

rpm:
	mkdir -p $(PWD)/rpmbuild/SOURCES/$(BIN_NAME)
	go build
	@cp ./$(NAME)*  $(PWD)/rpmbuild/SOURCES/$(BIN_NAME)
	cd $(PWD)/rpmbuild/SOURCES && tar cvfz $(BIN_NAME).tar.gz $(BIN_NAME)
	rpmbuild --define '_rpmfilename $(BIN_NAME).rpm' --define '_topdir $(PWD)/rpmbuild' --define 'version $(VERSION)' --define 'release $(RELEASE)' -ba --clean  $(NAME).spec

clean:
	rm -rf rpmbuild/SOURCES/yig-billing*
