#!/bin/bash

if [ -f ~/.ssh/id_rsa ]; then
	git push git@github.com:rawler/bhindex.git $TRAVIS_COMMIT:master
	git push git+ssh://rawler@git.launchpad.net/bhindex $TRAVIS_COMMIT:master
fi

