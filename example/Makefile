TARGET := example

DILL_VSN := 0.2.4
DILL := dill-$(DILL_VSN)
DILL_URL := https://github.com/uqfoundation/dill/releases/download/$(DILL)/$(DILL).tgz

PARAMIKO_VSN := 1.16.0
PARAMIKO := paramiko-$(PARAMIKO_VSN)
PARAMIKO_URL := https://github.com/paramiko/paramiko/archive/$(PARAMIKO_VSN).tar.gz

ECDSA_VSN := 0.13
ECDSA := python-ecdsa-$(ECDSA_VSN)
ECDSA_URL := https://github.com/warner/python-ecdsa/archive/$(ECDSA).tar.gz

PYSRC := $(shell find . -iname '*.py')


#
# Build zipapp
#

$(TARGET): $(TARGET).zip
	echo '#!/usr/bin/env python2' >$(TARGET)
	cat $< >>$(TARGET)
	chmod +x $(TARGET)

$(TARGET).zip: $(PYSRC) dill paramiko ecdsa
	rm -f $(TARGET).zip
	zip $@ $(PYSRC) $(VSN) $(shell find . -iname '*py')


#
# Download Dependencies
#

$(DILL).tgz:
	wget $(DILL_URL)

dill: $(DILL).tgz
	tar zxf $(DILL).tgz $(DILL)/dill
	mv $(DILL)/dill dill
	rmdir $(DILL)
	touch dill

$(PARAMIKO_VSN).tar.gz:
	wget $(PARAMIKO_URL)

paramiko: $(PARAMIKO_VSN).tar.gz
	tar zxvf $(PARAMIKO_VSN).tar.gz $(PARAMIKO)/paramiko
	mv $(PARAMIKO)/paramiko paramiko
	rmdir $(PARAMIKO)
	touch paramiko

$(ECDSA).tar.gz:
	wget $(ECDSA_URL)

ecdsa: $(ECDSA).tar.gz
	tar zxf $(ECDSA).tar.gz python-ecdsa-$(ECDSA)/ecdsa
	mv python-ecdsa-$(ECDSA)/ecdsa ecdsa
	rmdir python-ecdsa-$(ECDSA)
	touch ecdsa


#
# Cleanup
#

clean:
	rm -f $(TARGET).zip $(TARGET) $(VSN)
	find . -name "*.pyc" | xargs --no-run-if-empty rm
